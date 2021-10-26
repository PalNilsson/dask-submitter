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
from json import dump as dumpjson

logger = logging.getLogger(__name__)


def establish_logging(debug=True, nopilotlog=False, filename="dask-submitter.stdout", loglevel=0):
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


def create_namespace(_namespace, filename):
    """
    Create a namespace for this dask user.

    :param _namespace: namespace (string).
    :param filename: namespace json file name (string).
    :return: True if successful, stderr (Boolean, string).
    """

    namespace_dictionary = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata":
            {
                "name": _namespace, "labels":
                {
                    "name": _namespace
                }
            }
    }

    status = write_json(filename, namespace_dictionary)
    if not status:
        return False

    status, _, stderr = kubectl_apply(filename=filename)

    return status, stderr


def execute(executable, **kwargs):
    """
    Execute the command and its options in the provided executable list.
    The function also determines whether the command should be executed within a container.
    TODO: add time-out functionality.

    :param executable: command to be executed (string or list).
    :param kwargs (timeout, usecontainer, returnproc):
    :return: exit code (int), stdout (string) and stderr (string) (or process if requested via returnproc argument)
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
                               preexec_fn=os.setsid,
                               encoding='utf-8',
                               errors='replace'
                               )
    if returnproc:
        return process
    else:
        stdout, stderr = process.communicate()
        exit_code = process.poll()

        return exit_code, stdout, stderr


def kubectl_create(filename=None):
    """
    Execute the kubectl create command for a given yaml file.

    :param filename: yaml or json file name (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not filename:
        return None

    return kubectl_execute(cmd='create', filename=filename)


def kubectl_apply(filename=None):
    """
    Execute the kubectl apply command for a given yaml or json file.

    :param filename: yaml or json file name (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not filename:
        return None

    return kubectl_execute(cmd='apply', filename=filename)


def kubectl_delete(filename=None):
    """
    Execute the kubectl delete command for a given yaml file.

    :param filename: yaml file name (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not filename:
        return None

    return kubectl_execute(cmd='delete', filename=filename)


def kubectl_logs(pod=None, namespace=None):
    """
    Execute the kubectl logs command for a given pod name.

    :param pod: pod name (string).
    :param namespace: namespace (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not pod:
        return None, '', 'pod not set'
    if not namespace:
        return None, '', 'namespace not set'

    return kubectl_execute(cmd='logs', pod=pod, namespace=namespace)


def kubectl_execute(cmd=None, filename=None, pod=None, namespace=None):
    """
    Execute the kubectl create command for a given yaml file or pod name.

    :param cmd: kubectl command (string).
    :param filename: yaml or json file name (string).
    :param pod: pod name (string).
    :param namespace: namespace (string).
    :return: True if success, stdout, stderr (Boolean, string, string).
    """

    if not cmd:
        stderr = 'kubectl command not set not set'
        logger.warning(stderr)
        return None, '', stderr
    if cmd not in ['create', 'delete', 'logs', 'get pods', 'config use-context', 'apply']:
        stderr = 'unknown kubectl command: %s', cmd
        logger.warning(stderr)
        return None, '', stderr

    if cmd in ['create', 'delete', 'apply']:
        execmd = 'kubectl %s -f %s' % (cmd, filename)
    elif cmd == 'config use-context':
        execmd = 'kubectl %s %s' % (cmd, namespace)
    else:
        execmd = 'kubectl %s %s' % (cmd, pod) if pod else 'kubectl %s' % cmd

    if cmd in ['get pods', 'logs']:
        execmd += ' --namespace=%s' % namespace

    #logger.debug('executing: %s', execmd)
    exitcode, stdout, stderr = execute(execmd)
#    if exitcode and stderr.lower().startswith('error'):
    if exitcode and stderr:
        logger.warning('failed:\n%s', stderr)
        status = False
    else:
        status = True

    return status, stdout, stderr


def get_pod_name(namespace=None, pattern=r'(dask\-scheduler\-.+)'):
    """
    Find the name of the pod for the given name pattern in the output from the 'kubectl get pods' command.
    Note: this is intended for findout out the dask-scheduler name. It will not work for dask-workers since there
    will be multiple will similar names (dask-worker-<number>). There is only one scheduler.

    :param namespace: current namespace (string).
    :param pattern: pod name pattern (raw string).
    :return: pod name (string).
    """

    podname = ''

    cmd = 'kubectl get pods --namespace %s' % namespace
    exitcode, stdout, stderr = execute(cmd)

    if stderr:
        logger.warning('failed:\n%s', stderr)
        return podname
    dictionary = _convert_to_dict(stdout)

    if dictionary:
        for name in dictionary:
            _name = re.findall(pattern, name)
            if _name:
                podname = _name[0]
                break

    return podname


def wait_until_deployment(name=None, state=None, timeout=120, namespace=None, deployment=False):
    """
    Wait until a given pod or service is in running state.
    In case the service has an external IP, return it.

    Example: name=dask-pilot, state='Running', timeout=120. Function will wait a maximum of 120 s for the
    dask-pilot pod to reach Running state.

    :param name: pod or service name (string).
    :param state: optional pod status (string).
    :param timeout: time-out (integer).
    :param namespace: namespace (string).
    :param deployment: True for deployments (Boolean).
    :return: True if pod reaches given state before given time-out (Boolean), external IP (string), stderr (string).
    """

    if not name:
        return None, 'unset pod/service'

    _external_ip = None
    stderr = ''
    starttime = time.time()
    now = starttime
    _state = None
    _sleep = 5
    first = True
    processing = True
    podtype = 'deployment' if deployment else 'pod'
    podtype = '' if 'svc' in name else podtype  # do not specify podtype for a service
    ip_pattern = r'[0-9]+(?:\.[0-9]+){3}'  # e.g. 1.2.3.4
    port_pattern = r'([0-9]+)\:.'  # e.g. 80:30525/TCP
    while processing and (now - starttime < timeout):

        resource = 'services' if 'svc' in name else name
        cmd = "kubectl get %s %s --namespace=%s" % (podtype, resource, namespace)
        #logger.debug('executing cmd=\'%s\'', cmd)
        exitcode, stdout, stderr = execute(cmd)
        if stderr and stderr.lower().startswith('error'):
            logger.warning('failed:\n%s', stderr)
            break

        dictionary = _convert_to_dict(stdout)
        if dictionary:
            for _name in dictionary:  # e.g. _name = dask-scheduler-svc, kubernetes
                _dic = dictionary.get(_name)
                if 'STATUS' in _dic:
                    _state = _dic.get('STATUS')
                    if _state == state:
                        logger.info('%s is running', _name)
                        processing = False
                        break
                if 'EXTERNAL-IP' in _dic and name == _name:  # only look at the load balancer info (dask-scheduler-svc)
                    _ip = _dic.get('EXTERNAL-IP')
                    ip_number = re.findall(ip_pattern, _ip)
                    if ip_number:
                        _external_ip = ip_number[0]
                        # add the port (e.g. PORT(S)=80:30525/TCP)
                if 'PORT(S)' in _dic and name == _name:
                    _port = _dic.get('PORT(S)')
                    port_number = re.findall(port_pattern, _port)
                    if port_number and _external_ip:
                        _external_ip += ':%s' % port_number[0]
                        processing = False
                        break
                if first:
                    logger.info('sleeping until service is running (timeout=%d s)', timeout)
                    first = False
        time.sleep(_sleep)

        now = time.time()

    status = True if (_state and _state == state) else False
    return status, _external_ip, stderr


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


def write_json(filename, data, sort_keys=True, indent=4, separators=(',', ': ')):
    """
    Write the dictionary to a JSON file.

    :param filename: file name (string).
    :param data: object to be written to file (dictionary or list).
    :param sort_keys: should entries be sorted? (boolean).
    :param indent: indentation level, default 4 (int).
    :param separators: field separators (default (',', ': ') for dictionaries, use e.g. (',\n') for lists) (tuple)
    :raises PilotException: FileHandlingFailure.
    :return: status (boolean).
    """

    status = False

    try:
        with open(filename, 'w') as fh:
            dumpjson(data, fh, sort_keys=sort_keys, indent=indent, separators=separators)
    except IOError as exc:
        #raise FileHandlingFailure(exc)
        logger.warning('caught exception: %s', exc)
    else:
        status = True

    return status


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


def get_pv_yaml(namespace=None, user_id=None, nfs_server='10.226.152.66'):
    """

    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param nfs_server: NFS server IP (string).
    :return: yaml (string).
    """

    if not namespace:
        logger.warning('namespace must be set')
        return ""
    if not user_id:
        logger.warning('user id must be set')
        return ""

    yaml = """
apiVersion: v1
kind: PersistentVolume
metadata:
  name: fileserver-CHANGE_USERID
  namespace: CHANGE_NAMESPACE
spec:
  capacity:
    storage: 200Gi
  accessModes:
    - ReadWriteMany
  nfs:
    server: CHANGE_NFSSERVER
    path: "/vol1"
"""

    yaml = yaml.replace('CHANGE_USERID', user_id)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_NFSSERVER', nfs_server)

    return yaml


def get_pvc_yaml(namespace=None, user_id=None):
    """

    :param namespace: namespace (string).
    :param user_id: user id (string).
    :return: yaml (string).
    """

    if not namespace:
        logger.warning('namespace must be set')
        return ""
    if not user_id:
        logger.warning('user id must be set')
        return ""

    yaml = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: fileserver-claim
  namespace: CHANGE_NAMESPACE
spec:
  # Specify "" as the storageClassName so it matches the PersistentVolume's StorageClass.
  # A nil storageClassName value uses the default StorageClass. For details, see
  # https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
  accessModes:
  - ReadWriteMany
  storageClassName: ""
  volumeName: fileserver-CHANGE_USERID
  resources:
    requests:
      storage: 200Gi
"""

    yaml = yaml.replace('CHANGE_USERID', user_id)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)

    return yaml


def get_service_yaml(namespace=None, name=None, port=80, targetport=8786):
    """
    Return the yaml for the dask-scheduler load balancer service.

    :param namespace: namespace (string).
    :param name: service name (string).
    :param port: port (int).
    :param targetport: target port (default dask scheduler port, 8786) (int).
    :return: yaml (string).
    """

    if not namespace:
        logger.warning('namespace must be set')
        return ""
    if not name:
        logger.warning('service name must be set')
        return ""

    yaml = """
apiVersion: v1
kind: Service
metadata:
  name: CHANGE_SERVICENAME
  namespace: CHANGE_NAMESPACE
spec:
  type: LoadBalancer
  selector:
    app: CHANGE_SERVICENAME
    env: prod
  ports:
  - protocol: TCP
    port: CHANGE_PORT
    targetPort: CHANGE_TARGETPORT
"""

    yaml = yaml.replace('CHANGE_SERVICENAME', name)
    yaml = yaml.replace('CHANGE_PORT', str(port))
    yaml = yaml.replace('CHANGE_TARGETPORT', str(targetport))
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)

    return yaml


def get_scheduler_yaml(image_source=None, nfs_path=None, namespace=None, user_id=None, port=8786):
    """
    Return the yaml for the Dask scheduler for a given image and the path to the shared file system.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param port: optional container port (int).
    :return: yaml (string).
    """

    if not image_source:
        logger.warning('image source must be set')
        return ""
    if not nfs_path:
        logger.warning('nfs path must be set')
        return ""
    if not namespace:
        logger.warning('namespace must be set')
        return ""
    if not user_id:
        logger.warning('user id must be set')
        return ""

    yaml = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dask-scheduler
  namespace: CHANGE_NAMESPACE
  labels:
    app: dask-scheduler
spec:
  #replicas: 0
  selector:
    matchLabels:
      run: dask-scheduler
  template:
    metadata:
      labels:
        run: dask-scheduler
    spec:
      securityContext:
        runAsUser: 0
      containers:
      - name: dask-scheduler
        image: CHANGE_IMAGE_SOURCE
        volumeMounts:
        - mountPath: CHANGE_NFS_PATH
          name: fileserver-CHANGE_USERID
      volumes:
      - name: fileserver-CHANGE_USERID
        persistentVolumeClaim:
          claimName: fileserver-claim
          readOnly: false
"""

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_USERID', user_id)

    return yaml


def get_jupyterlab_yaml(image_source=None, nfs_path=None, namespace=None, user_id=None, port=8888):
    """
    Return the yaml for jupyterlab for a given image and the path to the shared file system.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :return: yaml (string).
    """

    if not image_source:
        logger.warning('image source must be set')
        return ""
    if not nfs_path:
        logger.warning('nfs path must be set')
        return ""
    if not namespace:
        logger.warning('namespace must be set')
        return ""
    if not user_id:
        logger.warning('user id must be set')
        return ""

    yaml = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jupyterlab
  namespace: CHANGE_NAMESPACE
  labels:
    name: jupyterlab
spec:
  #replicas: 0
  selector:
    matchLabels:
      name: jupyterlab
  template:
    metadata:
      labels:
        name: jupyterlab
    spec:
      securityContext:
        runAsUser: 0
        fsGroup: 0
      containers:
        - name: jupyterlab
          image: CHANGE_IMAGE_SOURCE
          imagePullPolicy: IfNotPresent
          ports:
          - containerPort: CHANGE_PORT
          command:
            - /bin/bash
            - -c
            - |
              start.sh jupyter lab --LabApp.token='password' --LabApp.ip='0.0.0.0' --LabApp.allow_root=True
          volumeMounts:
            - name: fileserver-CHANGE_USERID
              mountPath: CHANGE_NFS_PATH
      restartPolicy: Always
      volumes:
      - name: fileserver-CHANGE_USERID
        persistentVolumeClaim:
          claimName: fileserver-claim
"""
    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_PORT', str(port))
    yaml = yaml.replace('CHANGE_USERID', user_id)

    return yaml


def get_worker_yaml(image_source=None, nfs_path=None, scheduler_ip=None, worker_name=None, namespace=None, user_id=None):
    """
    Return the yaml for the Dask worker for a given image, path to the shared file system and Dask scheduler IP.

    Note: do not generate random worker names, use predictable worker_name; e.g. dask-worker-00001 etc
    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param scheduler_ip: dask scheduler IP (string).
    :param worker_name: dask worker name (string).
    :param namespace: namespace (string).
    :param user_id: user id (string).
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
    if not namespace:
        logger.warning('namespace must be set')
        return ""
    if not user_id:
        logger.warning('user id must be set')
        return ""

# image_source=palnilsson/dask-worker:latest
# scheduler_ip=e.g. tcp://10.8.2.3:8786
    yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: CHANGE_WORKER_NAME
  namespace: CHANGE_NAMESPACE
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
      name: fileserver-CHANGE_USERID
  volumes:
  - name: fileserver-CHANGE_USERID
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
"""

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_DASK_SCHEDULER_IP', scheduler_ip)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_WORKER_NAME', worker_name)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_USERID', user_id)

    return yaml


def get_pilot_yaml(image_source=None, nfs_path=None, namespace=None, scheduler_ip=None, user_id=None, panda_id=None):
    """
    Return the yaml for the Pilot X for a given image and the path to the shared file system.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param namespace: namespace (string).
    :param scheduler_ip: dask scheduler IP (string).
    :param user_id: user id (string).
    :param panda_id: PanDA id (string).
    :return: yaml (string).
    """

    if not image_source:
        logger.warning('image source must be set')
        return ""
    if not nfs_path:
        logger.warning('nfs path must be set')
        return ""
    if not namespace:
        logger.warning('namespace must be set')
        return ""
    if not scheduler_ip:
        logger.warning('dask scheduler IP must be set')
        return ""
    if not user_id:
        logger.warning('user id must be set')
        return ""
    if not panda_id:
        logger.warning('PanDA id must be set')
        return ""

    yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: dask-pilot
  namespace: CHANGE_NAMESPACE
spec:
  restartPolicy: Never
  containers:
  - name: dask-pilot
    image: CHANGE_IMAGE_SOURCE
    env:
    - name: DASK_SCHEDULER_IP
      value: "CHANGE_DASK_SCHEDULER_IP"
    - name: DASK_SHARED_FILESYSTEM_PATH
      value: "CHANGE_NFS_PATH"
    - name: PANDA_ID
      value: "CHANGE_PANDA_ID"
    volumeMounts:
    - mountPath: CHANGE_NFS_PATH
      name: fileserver-CHANGE_USERID
  volumes:
  - name: fileserver-CHANGE_USERID
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
"""

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_DASK_SCHEDULER_IP', scheduler_ip)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_PANDA_ID', panda_id)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_USERID', user_id)

    return yaml


def get_scheduler_info(timeout=480, namespace=None):
    """
    Wait for the scheduler to start, then grab the scheduler IP from the stdout and its proper pod name.

    :param timeout: time-out (integer).
    :param namespace: namespace (string).
    :return: scheduler IP (string), pod name (string), stderr (string).
    """

    scheduler_ip = ""

    podname = get_pod_name(namespace=namespace, pattern=r'(dask\-scheduler\-.+)')
    status, _, stderr = wait_until_deployment(name=podname, state='Running', timeout=120, namespace=namespace, deployment=False)
    if not status:
        return scheduler_ip, podname, stderr

    starttime = time.time()
    now = starttime
    _sleep = 5  # sleeping time between attempts
    first = True
    while now - starttime < timeout:
        # get the scheduler stdout
        status, stdout, stderr = kubectl_logs(pod=podname, namespace=namespace)
        if not status or not stdout:
            logger.warning('failed to extract scheduler IP from kubectl logs command: %s', stderr)
            return scheduler_ip, podname, stderr

        pattern = r'tcp://[0-9]+(?:\.[0-9]+){3}:[0-9]+'
        for line in stdout.split('\n'):
            if "Scheduler at:" in line:
                _ip = re.findall(pattern, line)
                if _ip:
                    scheduler_ip = _ip[0]
                    break

        if scheduler_ip:
            break
        else:
            # IP has not yet been extracted, wait longer and try again
            if first:
                logger.info('sleeping until scheduler IP is known (timeout=%d s)', timeout)
                first = False
            time.sleep(_sleep)
            now = time.time()

    return scheduler_ip, podname, ''


def get_jupyterlab_info(timeout=120, namespace=None):
    """
    Wait for the jupyterlab pod to start and its proper pod name.

    :param timeout: time-out (integer).
    :param namespace: namespace (string).
    :return: unused string (string), pod name (string), stderr (string).
    """

    podname = get_pod_name(namespace=namespace, pattern=r'(jupyterlab\-.+)')
    _, _, stderr = wait_until_deployment(name=podname, state='Running', timeout=120, namespace=namespace, deployment=False)
    if stderr:
        return '', podname, stderr

    starttime = time.time()
    now = starttime
    _sleep = 5  # sleeping time between attempts
    first = True
    started = False
    while now - starttime < timeout:
        # get the scheduler stdout
        status, stdout, stderr = kubectl_logs(pod=podname, namespace=namespace)
        if not status or not stdout:
            logger.warning('jupyterlab pod stdout:\n%s', stdout)
            logger.warning('jupyterlab pod failed to start: %s', stderr)
            return '', podname, stderr

        for line in stdout.split('\n'):
            if "Jupyter Server" in line and 'is running at:' in line:
                logger.info('jupyter server is running')
                started = True
                break

        if started:
            break

        # IP has not yet been extracted, wait longer and try again
        if first:
            logger.info('sleeping until jupyter server has started (timeout=%d s)', timeout)
            first = False
        time.sleep(_sleep)
        now = time.time()

    if not started:
        stderr = 'Jupyter server did not start'
    return '', podname, stderr


def deploy_workers(scheduler_ip, _nworkers, yaml_files, namespace, user_id, imagename, mountpath):
    """
    Deploy the worker pods and return a dictionary with the worker info.

    worker_info = { worker_name_%d: yaml_path_%d, .. }

    :param scheduler_ip: dask scheduler IP (string).
    :param _nworkers: number of workers (int).
    :param yaml_files: yaml files dictionary.
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param imagename: image name (string).
    :param mountpath: FS mount path (string).
    :return: worker info dictionary, stderr (dictionary, string).
    """

    worker_info = {}
    for _iworker in range(_nworkers):

        worker_name = 'dask-worker-%d' % _iworker
        worker_path = os.path.join(os.getcwd(), yaml_files.get('dask-worker') % _iworker)
        worker_info[worker_name] = worker_path

        # create worker yaml
        worker_yaml = get_worker_yaml(image_source=imagename,
                                      nfs_path=mountpath,
                                      scheduler_ip=scheduler_ip,
                                      worker_name=worker_name,
                                      namespace=namespace,
                                      user_id=user_id)
        status = write_file(worker_path, worker_yaml, mute=False)
        if not status:
            stderr = 'cannot continue since yaml file could not be created'
            logger.warning(stderr)
            return None, stderr

        # start the worker pod
        status, _, stderr = kubectl_create(filename=worker_path)
        if not status:
            return None, stderr

        logger.info('deployed dask-worker-%d pod', _iworker)

    return worker_info, ''


def await_worker_deployment(worker_info, namespace, scheduler_pod_name='', jupyter_pod_name='', timeout=300):
    """
    Wait for all workers to start running.

    :param worker_info: worker info dictionary.
    :param namespace: namespace (string).
    :param scheduler_pod_name: pod name for scheduler (string).
    :param timeout: optional time-out (int).
    :return: True if all pods end up in Running state (Boolean), stderr (string).
    """

    running_workers = []

    stderr = ''
    starttime = time.time()
    now = starttime
    _state = None
    _sleep = 5
    processing = True
    status = True
    while processing and (now - starttime < timeout):

        # get the full pod info dictionary - note: not good if MANY workers
        status, stdout, stderr = kubectl_execute(cmd='get pods', namespace=namespace)
        if not status:
            break

        # convert command output to a dictionary
        dictionary = _convert_to_dict(stdout)

        # get list of workers and get rid of the scheduler and workers that are already known to be running
        workers_list = list(dictionary.keys())
        if scheduler_pod_name:
            workers_list.remove(scheduler_pod_name)
        if jupyter_pod_name:
            workers_list.remove(jupyter_pod_name)
        for running_worker in running_workers:
            workers_list.remove(running_worker)

        # check the states
        for worker_name in workers_list:
            # is the worker in Running state?
            try:
                state = dictionary[worker_name]['STATUS']
            except Exception as exc:
                stderr = 'caught exception: %s', exc
                logger.warning(stderr)
            else:
                if state == 'Running':
                    running_workers.append(worker_name)
        if len(running_workers) == len(list(dictionary.keys())) - 2:
            logger.info('all workers are running')
            processing = False
        else:
            time.sleep(_sleep)
            now = time.time()

    logger.debug('number of running dask workers: %d', len(running_workers))

    return status
