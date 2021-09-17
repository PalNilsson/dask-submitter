#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

import logging
import os
import re
import subprocess
import sys
import time

logger = logging.getLogger(__name__)


def establish_logging(debug=True, nopilotlog=False, filename="dasksubmitter.stdout", loglevel=0):
    """
    Setup and establish logging.

    Option loglevel can be used to decide which (predetermined) logging format to use.
    Example:
      loglevel=0: '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
      loglevel=1: 'ts=%(asctime)s level=%(levelname)-8s event=%(name)-32s.%(funcName)-25s msg="%(message)s"'

    :param debug: debug mode (Boolean).
    :param nopilotlog: True when pilot log is not known (Boolean).
    :param filename: name of log file (string).
    :param loglevel: selector for logging level (int).
    :return:
    """

    _logger = logging.getLogger('')
    _logger.handlers = []
    _logger.propagate = False

    console = logging.StreamHandler(sys.stdout)
    if debug:
        format_str = '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
        level = logging.DEBUG
    else:
        format_str = '%(asctime)s | %(levelname)-8s | %(message)s'
        level = logging.INFO

    if nopilotlog:
        logging.basicConfig(level=level, format=format_str, filemode='w')
    else:
        logging.basicConfig(filename=filename, level=level, format=format_str, filemode='w')
    console.setLevel(level)
    console.setFormatter(logging.Formatter(format_str))
    logging.Formatter.converter = time.gmtime
    _logger.addHandler(console)


def execute(executable, **kwargs):
    """
    Execute the command and its options in the provided executable list.
    The function also determines whether the command should be executed within a container.
    TODO: add time-out functionality.

    :param executable: command to be executed (string or list).
    :param kwargs (timeout, usecontainer, returnproc):
    :return: exit code, stdout and stderr (or process if requested via returnproc argument)
    """

    cwd = kwargs.get('cwd', os.getcwd())
    stdout = kwargs.get('stdout', subprocess.PIPE)
    stderr = kwargs.get('stderr', subprocess.PIPE)
    timeout = kwargs.get('timeout', 120)
    usecontainer = kwargs.get('usecontainer', False)
    returnproc = kwargs.get('returnproc', False)
    job = kwargs.get('job')

    # convert executable to string if it is a list
    if type(executable) is list:
        executable = ' '.join(executable)

    exe = ['/bin/bash', '-c', executable]
    process = subprocess.Popen(exe,
                               bufsize=-1,
                               stdout=stdout,
                               stderr=stderr,
                               cwd=cwd,
                               preexec_fn=os.setsid)
    if returnproc:
        return process
    else:
        stdout, stderr = process.communicate()
        exit_code = process.poll()

        return exit_code, stdout, stderr


def deploy(yaml=None):
    """
    Execute the kubectl create command for a given yaml file.

    :param yaml: yaml file name (string).
    :return: True if success (Boolean).
    """

    if not yaml:
        return None

    cmd = 'kubectl create -f %s' % yaml
    exitcode, stdout, stderr = execute(cmd)
    if exitcode and stderr.lower().startswith('error'):
        logger.warning('failed:\n%s', stderr)
        status = False
    else:
        logger.info('deployed %s', yaml)
        status = True

    return status


def wait_until_deployment(pod=None, state=None, timeout=120):
    """
    Wait until a given pod is in running state.

    Example: pod=dask-pilot, status='Running', timeout=120. Function will wait a maximum of 120 s for the
    dask-pilot pod to reach Running state.

    :param pod: pod name (string).
    :param state: required status (string).
    :param timeout: time-out (integer).
    :return: True if pod reaches given state before given time-out (Boolean).
    """

    if not pod or not state:
        return None

    starttime = time.time()
    now = starttime
    processing = True
    _state = None
    while processing and (now - starttime < timeout):

        exitcode, stdout, stderr = execute("kubectl get pod %s" % pod)
        if stderr and stderr.lower().startswith('error:'):
            logger.warning('failed:\n%s', stderr)
            break

        dictionary = _convert_to_dict(stdout)
        if dictionary:
            for name in dictionary:
                logger.info('processing status of %s', name)
                _dic = dictionary.get(name)
                if 'STATUS' in _dic:
                    _state = _dic.get('STATUS')
                    if _state == state:
                        processing = False
                        break

                time.sleep(5)

        now = time.time()

    return True if (_state and _state == state) else False


def _convert_to_dict(stdout):
    """
    Convert table-like stdout to a dictionary.

    :param stdout: command output (string).
    :return: formatted dictionary.
    """

    dictionary = {}
    first_line = []
    for line in stdout.split('\n'):
        if not line:
            continue
        try:
            # Remove empty entries from list (caused by multiple \t)
            _l = re.sub(' +', ' ', line)
            _l = [_f for _f in _l.split(' ') if _f]
            # NAME READY STATUS RESTARTS AGE
            if first_line == []:
                first_line = _l[1:]
            else:
                dictionary[_l[0]] = {}
                for i in range(len(_l[1:])):
                    dictionary[_l[0]][first_line[i]] = _l[1:][i]

        except Exception:
            logger.warning("unexpected format of utility output: %s", line)

    return dictionary


def open_file(filename, mode):
    """
    Open and return a file pointer for the given mode.
    Note: the caller needs to close the file.

    :param filename: file name (string).
    :param mode: file mode (character).
    :raises PilotException: FileHandlingFailure.
    :return: file pointer.
    """

    f = None
    try:
        f = open(filename, mode)
    except IOError as exc:
        logger.warning('caught exception: %s', exc)
        #raise FileHandlingFailure(exc)

    return f


def write_file(path, contents, mute=True, mode='w', unique=False):
    """
    Write the given contents to a file.
    If unique=True, then if the file already exists, an index will be added (e.g. 'out.txt' -> 'out-1.txt')
    :param path: full path for file (string).
    :param contents: file contents (object).
    :param mute: boolean to control stdout info message.
    :param mode: file mode (e.g. 'w', 'r', 'a', 'wb', 'rb') (string).
    :param unique: file must be unique (Boolean).
    :raises PilotException: FileHandlingFailure.
    :return: True if successful, otherwise False.
    """

    status = False

    # add an incremental file name (add -%d if path already exists) if necessary
    #if unique:
    #    path = get_nonexistant_path(path)

    f = open_file(path, mode)
    if f:
        try:
            f.write(contents)
        except IOError as exc:
            logger.warning('caught exception: %s', exc)
            #raise FileHandlingFailure(exc)
        else:
            status = True
        f.close()

    if not mute:
        if 'w' in mode:
            logger.info('created file: %s', path)
        if 'a' in mode:
            logger.info('appended file: %s', path)

    return status


def get_scheduler_yaml(image_source="", nfs_path=""):
    """
    Return the yaml for the Dask scheduler for a given image and the path to the shared file system.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :return: yaml (string).
    """

    if not image_source:
        logger.warning('image source must be set')
        return ""
    if not nfs_path:
        logger.warning('nfs path must be set')
        return ""

    yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: dask-scheduler
spec:
  restartPolicy: Never
  containers:
  - name: dask-scheduler
    image: CHANGE_IMAGE_SOURCE
    volumeMounts:
    - mountPath: CHANGE_NFS_PATH
      name: fileserver
  volumes:
  - name: fileserver
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
    """

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)

    return yaml


def get_worker_yaml(image_source="", nfs_path="", scheduler_ip="", worker_name=""):
    """
    Return the yaml for the Dask worker for a given image, path to the shared file system and Dask scheduler IP.

    Note: do not generate random worker names, use predictable worker_name; e.g. dask-worker-00001 etc
    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param scheduler_ip: dask scheduler IP (string).
    :param worker_name: dask worker name (string).
    :return: yaml (string).
    """

    if not image_source:
        logger.warning('image source must be set')
        return ""
    if not nfs_path:
        logger.warning('nfs path must be set')
        return ""
    if not scheduler_ip:
        logger.warning('dask scheduler IP must be set')
        return ""
    if not worker_name:
        logger.warning('dask worker name must be set')
        return ""

# image_source=palnilsson/dask-worker:latest
# scheduler_ip=e.g. tcp://10.8.2.3:8786
    yaml = """
apiVersion: v1
kind: Pod
spec:
  restartPolicy: Never
  containers:
  - name: CHANGE_WORKER_NAME
    image: CHANGE_IMAGE_SOURCE
    env:
    - name: DASK_SCHEDULER_IP
      value: "CHANGE_DASK_SCHEDULER_IP"
    - name: DASK_SHARED_FILESYSTEM_PATH
      value: CHANGE_NFS_PATH
    volumeMounts:
    - mountPath: CHANGE_NFS_PATH
      name: fileserver
  volumes:
  - name: fileserver
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
    """

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_DASK_SCHEDULER_IP', scheduler_ip)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_WORKER_NAME', worker_name)

    return yaml


def get_pilot_yaml(image_source="", nfs_path=""):
    """
    Return the yaml for the Pilot X for a given image and the path to the shared file system.

    # palnilsson/dask-pilot:latest

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :return: yaml (string).
    """

    if not image_source:
        logger.warning('image source must be set')
        return ""
    if not nfs_path:
        logger.warning('nfs path must be set')
        return ""

    yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: dask-pilot
spec:
  restartPolicy: Never
  containers:
  - name: dask-pilot
    image: CHANGE_IMAGE_SOURCE
    env:
    - name: DASK_SHARED_FILESYSTEM_PATH
      value: CHANGE_NFS_PATH
    volumeMounts:
    - mountPath: CHANGE_NFS_PATH
      name: fileserver
  volumes:
  - name: fileserver
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
    """

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)

    return yaml
