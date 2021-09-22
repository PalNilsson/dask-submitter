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
    _namespace = 'single-user-%s' % ''.join(random.choice(ascii_lowercase) for _ in range(5))
    namespace_filename = os.path.join(os.getcwd(), 'namespace.json')
    status = utilities.create_namespace(_namespace, namespace_filename)
    if not status:
        logger.warning('failed to create namespace: %s', _namespace)
    else:
        logger.info('created namespace: %s', _namespace)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace=_namespace)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace='default')

    # remove the single-user namespace
    #status = utilities.kubectl_delete(filename=namespace_filename)

    # create scheduler yaml
    scheduler_path = os.path.join(os.getcwd(), yaml_files.get('dask-scheduler'))
    scheduler_yaml = utilities.get_scheduler_yaml(image_source="palnilsson/dask-scheduler:latest",
                                                  nfs_path="/mnt/dask",
                                                  namespace=_namespace)
    status = utilities.write_file(scheduler_path, scheduler_yaml, mute=False)
    if not status:
        logger.warning('cannot continue since yaml file could not be created')
        exit(-1)

    # start the dask scheduler pod
    status, _ = utilities.kubectl_create(filename=scheduler_path)
    if not status:
        exit(-1)
    logger.info('deployed dask-scheduler pod')

    # extract scheduler IP from stdout (when available)
    scheduler_ip = utilities.get_scheduler_ip(pod='dask-scheduler')
    if not scheduler_ip:
        exit(-1)
    logger.info('using dask-scheduler IP: %s', scheduler_ip)

    # deploy the worker pods
    _nworkers = 2  # from Dask object..
    worker_info = utilities.deploy_workers(scheduler_ip, _nworkers, yaml_files, _namespace)
    if not worker_info:
        exit(-1)

    # wait for the worker pods to start
    status = utilities.await_worker_deployment(worker_info, _namespace)
    if not status:
        exit(-1)

    #status = utilities.kubectl_delete(filename=scheduler_path)
    now = time.time()
    logger.info('total running time: %d s', now - starttime)
    exit(0)

    pod = 'dask-pilot'
    status = utilities.wait_until_deployment(pod=pod, state='Running')
    if not status:
        exit(-1)
    else:
        logger.info('pod %s is running', pod)

    # extract scheduler IP from stdout (when available)
    # ..

    exit(0)
