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
import random
import sys
import time
from string import ascii_lowercase

#try:
#    # import dask
#    import dask_kubernetes
##except ModuleNotFoundError:  # Python 3
#except Exception:
#    pass

#import re
#from time import sleep

import utilities

logger = logging.getLogger(__name__)


class DaskSubmitter(object):
    """
    Dask submitter interface class.
    """

    # note: all private fields can be set in init function except the _ispvc and _ispv

    _nworkers = 1
    _namespace = ''
    _userid = ''
    _mountpath = '/mnt/dask'
    _ispvc = False  # set when PVC is successfully created
    _ispv = False  # set when PV is successfully created

    _files = {
        'dask-scheduler-service': 'dask-scheduler-service.yaml',
        'dask-scheduler': 'dask-scheduler-deployment.yaml',
        'dask-worker': 'dask-worker-deployment-%d.yaml',
        'dask-pilot': 'dask-pilot-deployment.yaml',
        'namespace': 'namespace.json',
        'pvc': 'pvc.yaml',
        'pv': 'pv.yaml',
    }

    _images = {
        'dask-scheduler': 'palnilsson/dask-scheduler:latest',
        'dask-worker': 'palnilsson/dask-worker:latest',
        'dask-pilot': 'palnilsson/dask-pilot:latest'
    }

    _podnames = {
        'dask-scheduler-service': 'dask-scheduler-svc',
        'dask-scheduler': 'dask-scheduler',
        'dask-worker': 'dask-worker',
        'dask-pilot': 'dask-pilot',
    }

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        self._nworkers = kwargs.get('nworkers', 1)
        self._userid = kwargs.get('userid', ''.join(random.choice(ascii_lowercase) for _ in range(5)))
        self._namespace = kwargs.get('namespace', 'single-user-%s' % self._userid)
        self._files = kwargs.get('files', self._files)
        self._images = kwargs.get('images', self._images)

    def get_userid(self):
        """
        Return the user id.

        :return: user id (string).
        """

        return self._userid

    def get_namespace(self):
        """
        Return the namespace.

        namespace = single-user-<user id>.

        :return: namespace (string).
        """

        return self._namespace

    def create_namespace(self):
        """
        Create the random namespace.

        :return: True if successful, stderr (Boolean, string).
        """

        namespace_filename = os.path.join(os.getcwd(), self._files.get('namespace'))
        return utilities.create_namespace(self._namespace, namespace_filename)

    def create_pvcpv(self, name='pvc'):
        """
        Create the PVC or PV.

        :param name: 'pvc' or 'pv' (string).
        :return: True if successful (Boolean), stderr (string).
        """

        if name not in ['pvc', 'pv']:
            stderr = 'unknown PVC/PC name: %s', name
            logger.warning(stderr)
            return False, stderr

        # create the yaml file
        path = os.path.join(os.path.join(os.getcwd(), self._files.get(name)))
        func = utilities.get_pvc_yaml if name == 'pvc' else utilities.get_pv_yaml
        yaml = func(namespace=self._namespace, user_id=self._userid)
        status = utilities.write_file(path, yaml)
        if not status:
            return False, 'write_file failed for file %s' % path

        # create the PVC/PV
        status, _, stderr = utilities.kubectl_create(filename=path)
        if name == 'pvc':
            self._ispvc = status
        elif name == 'pv':
            self._ispv = status

        return status, stderr

    def deploy_dask_scheduler(self):
        """
        Deploy the dask scheduler and return its IP.

        :return: scheduler IP (string), scheduler pod name (string), stderr (string).
        """

        # create scheduler yaml
        scheduler_path = os.path.join(os.getcwd(), self._files.get('dask-scheduler'))
        scheduler_yaml = utilities.get_scheduler_yaml(image_source=self._images.get('dask-scheduler'),
                                                      nfs_path=self._mountpath,
                                                      namespace=self._namespace,
                                                      user_id=self._userid,
                                                      kind='Deployment')
        status = utilities.write_file(scheduler_path, scheduler_yaml, mute=False)
        if not status:
            stderr = 'cannot continue since yaml file could not be created'
            logger.warning(stderr)
            return '', '', stderr

        # start the dask scheduler pod
        status, _, stderr = utilities.kubectl_create(filename=scheduler_path)
        if not status:
            return status, '', stderr

        # extract scheduler IP from stdout (when available)
        return utilities.get_scheduler_info(namespace=self._namespace)

    def deploy_dask_workers(self, scheduler_ip, scheduler_pod_name):
        """
        Deploy all dask workers.

        :param scheduler_ip: dask scheduler IP (string).
        :param scheduler_pod_name: pod name for scheduler (string).
        :return: True if successful, stderr (Boolean, string)
        """

        worker_info, stderr = utilities.deploy_workers(scheduler_ip,
                                                       self._nworkers,
                                                       self._files,
                                                       self._namespace,
                                                       self._userid,
                                                       self._images.get('dask-worker'),
                                                       self._mountpath)
        if not worker_info:
            logger.warning('failed to deploy workers: %s', stderr)
            return False, stderr

        # wait for the worker pods to start
        try:
            status = utilities.await_worker_deployment(worker_info, scheduler_pod_name, self._namespace)
        except Exception as exc:
            stderr = 'caught exception: %s', exc
            logger.warning(stderr)
            status = False

        return status, stderr

    def deploy_pilot(self, scheduler_ip):
        """
        Deploy the pilot pod.

        :param scheduler_ip: dash scheduler IP (string).
        :return: True if successful (Boolean), [None], stderr (string).
        """

        # create pilot yaml
        path = os.path.join(os.getcwd(), self._files.get('dask-pilot'))
        yaml = utilities.get_pilot_yaml(image_source=self._images.get('dask-pilot'),
                                        nfs_path=self._mountpath,
                                        namespace=self._namespace,
                                        user_id=self._userid,
                                        scheduler_ip=scheduler_ip,
                                        panda_id='1234567890')
        status = utilities.write_file(path, yaml, mute=False)
        if not status:
            stderr = 'cannot continue since pilot yaml file could not be created'
            logger.warning(stderr)
            return False, stderr

        # start the pilot pod
        status, _, stderr = utilities.kubectl_create(filename=path)
        if not status:
            logger.warning('failed to create pilot pod: %s', stderr)
            return False, stderr
        else:
            logger.debug('created pilot pod')

        return utilities.wait_until_deployment(name=self._podnames.get('dask-pilot'), state='Running', namespace=self._namespace)

    def copy_bundle(self):
        """
        Copy bundle (incl. job definition).

        :return: True if successful (Boolean).
        """

        status = True

        return status

    def create_service(self):
        """
        Create dask-scheduler service yaml and start it.
        The function waits for the load balancer to expose the external IP and returns it.

        :return: external IP from load balancer (string), stderr (string)
        """

        _external_ip = ''
        _stderr = ''

        path = os.path.join(os.getcwd(), self._files.get('dask-scheduler-service'))
        yaml = utilities.get_service_yaml(namespace=self._namespace,
                                          name=self._podnames.get('dask-scheduler-service'),
                                          port=80,
                                          targetport=8786)
        status = utilities.write_file(path, yaml, mute=False)
        if not status:
            _stderr = 'cannot continue since dask-scheduler service yaml file could not be created'
            logger.warning(_stderr)
            return _external_ip, _stderr

        # start the dask-scheduler service
        status, _, _stderr = utilities.kubectl_create(filename=path)
        if not status:
            logger.warning('failed to create pilot pod: %s', _stderr)
            return _external_ip, _stderr
        else:
            logger.debug('created dask-scheduler service')

        status, _external_ip, _stderr = utilities.wait_until_deployment(name=self._podnames.get('dask-scheduler-service'), namespace=self._namespace)

        return _external_ip, _stderr


def cleanup(namespace=None, user_id=None, pvc=False, pv=False):
    """
    General cleanup.

    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param pvc: True if PVC was created (Boolean).
    :param pv: True if PV was created (Boolean).
    :return:
    """

    if namespace:
        cmd = 'kubectl delete --all pods --namespace=%s' % namespace
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

    if pvc:
        cmd = 'kubectl patch pvc fileserver-claim -p \'{\"metadata\": {\"finalizers\": null}}\' --namespace=%s' % namespace
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)
        cmd = 'kubectl delete pvc fileserver-claim --namespace=%s' % namespace
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

    if pv:
        cmd = 'kubectl patch pv fileserver-%s -p \'{\"metadata\": {\"finalizers\": null}}\'' % user_id
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)
        cmd = 'kubectl delete pv fileserver-%s' % user_id
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

    if namespace:
        cmd = 'kubectl delete namespaces %s' % namespace
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

    # terminate logging
    # ..


if __name__ == '__main__':

    # move this code to install()

    utilities.establish_logging()
    logging.info("*** Dask submitter ***")
    logging.info("Python version %s", sys.version)
    starttime = time.time()
    submitter = DaskSubmitter(nworkers=2)

    # create unique name space
    status, stderr = submitter.create_namespace()
    if not status:
        logger.warning('failed to create namespace %s: %s', submitter.get_namespace(), stderr)
        cleanup()
        exit(-1)
    else:
        logger.info('created namespace: %s', submitter.get_namespace())

    # create PVC and PV
    for name in ['pvc', 'pv']:
        status, stderr = submitter.create_pvcpv(name=name)
        if not status:
            logger.warning('could not create PVC/PV: %s', stderr)
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid())
            exit(-1)
    logger.info('created PVC and PV')

    # create the dask scheduler service with a load balancer (the external IP of the load balancer will be made
    # available to the caller)
    # [wait until external IP is available]
    external_ip, stderr = submitter.create_service()
    if not external_ip:
        logger.warning('failed to deploy dask scheduler load balancer (external IP not returned): %s', stderr)
        cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
        exit(-1)
    logger.info('deployed load balancer with external ip=%s', external_ip)

    # deploy the dask scheduler (the scheduler IP will only be available from within the cluster)
    scheduler_ip, scheduler_pod_name, stderr = submitter.deploy_dask_scheduler()
    if not scheduler_ip:
        logger.warning('failed to deploy dask scheduler: %s', stderr)
        cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
        exit(-1)
    logger.info('deployed %s pod', scheduler_pod_name)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace=namespace)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace='default')

    # deploy the worker pods
    status, stderr = submitter.deploy_dask_workers(scheduler_ip, scheduler_pod_name)
    if not status:
        logger.warning('failed to deploy dask workers: %s', stderr)
        cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
        exit(-1)
    logger.info('deployed all dask-worker pods')

    #######
    #from dask.distributed import Client
    #try:
    #    logger.debug('using scheduler ip=%s', scheduler_ip)
    #    client = Client(scheduler_ip)
    #except IOError as exc:
    #    logger.warning('failed to connect to dask submitter:\n%s', exc)
    #    #cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
    #    exit(-1)
    #else:
    #    logger.info('connected client to scheduler at %s', scheduler_ip)
    #######

    cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)

    # deploy the pilot pod
    status, _, stderr = submitter.deploy_pilot(scheduler_ip)

    # time.sleep(30)
    cmd = 'kubectl logs dask-pilot --namespace=%s' % submitter.get_namespace()
    logger.debug('executing: %s', cmd)
    ec, stdout, stderr = utilities.execute(cmd)
    logger.debug(stdout)

    if not status:
        cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
        exit(-1)
    logger.info('deployed pilot pod')

    # done, cleanup and exit
    now = time.time()
    logger.info('total running time: %d s', now - starttime)
    #cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
    exit(0)
