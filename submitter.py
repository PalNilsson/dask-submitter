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

    _nworkers = 1
    _namespace = ''

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        self._nworkers = kwargs.get('nworkers', 1)
        self._namespace = 'single-user-%s' % ''.join(random.choice(ascii_lowercase) for _ in range(5))

    def install(self, job_definition):
        """
        Install the pods for the dask scheduler and workers, and Pilot X

        Note: Pilot X is currently a simplified PanDA Pilot, but is likely to be absorbed into the main
        PanDA Pilot code base as a special workflow for Dask on Kubernetes resources.

        The install function will start by installing the Pilot X pod on the dask cluster. When it starts running,
        Pilot X will wait for a job definition to appear on the shared file system. It will then proceed staging any
        input files.

        In the meantime, this function (who also knows about the job definition) will asynchronously install the dask
        scheduler and all required workers

        :param job_definition: job definition dictionary.
        :return: True for successfully installed pods (Boolean).
        """

        # install Pilot X pod
        status = self.install_pilotx_pod()
        if not status:
            return status

        # copy bundle (job definition etc)

        # copy job definition to shared directory
        # (copy to Pilot X pod which has the shared directory mounted)
        status = self.copy_job_definition(job_definition)
        if not status:
            return status

        # install dask scheduler
        status = self.install_dask_scheduler()
        if not status:
            return status

        # install dask worker(s)
        status = self.install_dask_workers(job_definition)
        if not status:
            return status

        return status

    def uninstall(self):
        """
        Uninstall all pods.
        """

        # uninstall all pods
        # ..

        pass

    def install_pilotx_pod(self):
        """

        """

        status = True

        return status

    def copy_job_definition(self, job_definition):
        """

        """

        status = True

        return status

    def install_dask_scheduler(self):
        """

        """

        status = True

        return status

    def install_dask_workers(self, job_definition):
        """

        """

        status = True

        # get number of workers from job definition
        # issue all install commands at once, then wait for pod status to be 'running'

        return status


def cleanup(namespace=None, user_id=None, pvc=False, pv=False):
    """
    General cleanup.

    :param namespace: namespace (string).
    :param user_id: user id (string).
    :return:
    """

    if namespace:
        cmd = 'kubectl delete --all pods --namespace=%s' % namespace
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

    if pvc:
        cmd = 'kubectl patch pvc fileserver-claim -p \'{\"metadata\": {\"finalizers\": null}}\''
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
    submitter = DaskSubmitter()

    yaml_files = {
        'dask-scheduler': 'dask-scheduler-deployment.yaml',
        'dask-worker': 'dask-worker-deployment-%d.yaml',
        'dask-pilot': 'dask-pilot-deployment.yaml',
    }

    # create unique name space
    user_id = ''.join(random.choice(ascii_lowercase) for _ in range(5))
    namespace = 'single-user-%s' % user_id
    namespace_filename = os.path.join(os.getcwd(), 'namespace.json')
    status = utilities.create_namespace(namespace, namespace_filename)
    if not status:
        logger.warning('failed to create namespace: %s', namespace)
        cleanup()
        exit(-1)
    else:
        logger.info('created namespace: %s', namespace)
    #namespace = "default"

    # create PVC
    pvc_path = os.path.join(os.path.join(os.getcwd(), 'pvc.yaml'))
    pvc_yaml = utilities.get_pvc_yaml(namespace=namespace, user_id=user_id)
    status = utilities.write_file(pvc_path, pvc_yaml)
    if not status:
        logger.warning('cannot continue since yaml file could not be created')
        cleanup(namespace=namespace, user_id=user_id)
        exit(-1)

    #
    status, _ = utilities.kubectl_create(filename=pvc_path)
    if not status:
        cleanup(namespace=namespace, user_id=user_id)
        exit(-1)

    # create PV
    pv_path = os.path.join(os.path.join(os.getcwd(), 'pv.yaml'))
    pv_yaml = utilities.get_pv_yaml(namespace=namespace, user_id=user_id)
    status = utilities.write_file(pv_path, pv_yaml)
    if not status:
        logger.warning('cannot continue since yaml file could not be created')
        cleanup(namespace=namespace, user_id=user_id, pvc=True)
        exit(-1)

    #
    status, _ = utilities.kubectl_create(filename=pv_path)
    if not status:
        cleanup(namespace=namespace, user_id=user_id, pvc=True)
        exit(-1)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace=namespace)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace='default')

    # create scheduler yaml
    scheduler_path = os.path.join(os.getcwd(), yaml_files.get('dask-scheduler'))
    scheduler_yaml = utilities.get_scheduler_yaml(image_source="palnilsson/dask-scheduler:latest",
                                                  nfs_path="/mnt/dask",
                                                  namespace=namespace,
                                                  user_id=user_id)
    status = utilities.write_file(scheduler_path, scheduler_yaml, mute=False)
    if not status:
        logger.warning('cannot continue since yaml file could not be created')
        cleanup(namespace=namespace, user_id=user_id, pvc=True, pv=True)
        exit(-1)

    # start the dask scheduler pod
    status, _ = utilities.kubectl_create(filename=scheduler_path)
    if not status:
        cleanup(namespace=namespace, user_id=user_id, pvc=True, pv=True)
        exit(-1)
    logger.info('deployed dask-scheduler pod')

    # extract scheduler IP from stdout (when available)
    scheduler_ip = utilities.get_scheduler_ip(pod='dask-scheduler', namespace=namespace)
    if not scheduler_ip:
        cleanup(namespace=namespace, user_id=user_id, pvc=True, pv=True)
        exit(-1)
    logger.info('using dask-scheduler IP: %s', scheduler_ip)

    # deploy the worker pods
    _nworkers = 2  # from Dask object..
    worker_info = utilities.deploy_workers(scheduler_ip, _nworkers, yaml_files, namespace, user_id)
    if not worker_info:
        cleanup(namespace=namespace, user_id=user_id, pvc=True, pv=True)
        exit(-1)

    # wait for the worker pods to start
    try:
        status = utilities.await_worker_deployment(worker_info, namespace)
    except Exception as exc:
        logger.warning('caught exception: %s', exc)
        status = False
    if not status:
        cleanup(namespace=namespace, user_id=user_id, pvc=True, pv=True)
        exit(-1)

    pod = 'dask-pilot'
    status = utilities.wait_until_deployment(pod=pod, state='Running')
    if not status:
        cleanup(namespace=namespace, user_id=user_id, pvc=True, pv=True)
        exit(-1)
    else:
        logger.info('pod %s is running', pod)

    now = time.time()
    logger.info('total running time: %d s', now - starttime)

    cleanup(namespace=namespace, user_id=user_id, pvc=True, pv=True)
    exit(0)
