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
        'jupyterlab-service': 'jupyterlab-service.yaml',
        'jupyterlab': 'jupyterlab-deployment.yaml',
        'namespace': 'namespace.json',
        'pvc': 'pvc.yaml',
        'pv': 'pv.yaml',
    }

    _images = {
        'dask-scheduler': 'palnilsson/dask-scheduler:latest',
        'dask-worker': 'palnilsson/dask-worker:latest',
        'dask-pilot': 'palnilsson/dask-pilot:latest',
        'jupyterlab': 'jupyter/datascience-notebook:latest',
    }

    _podnames = {
        'dask-scheduler-service': 'dask-scheduler',
        'dask-scheduler': 'dask-scheduler',
        'dask-worker': 'dask-worker',
        'dask-pilot': 'dask-pilot',
        'jupyterlab-service': 'jupyterlab',
        'jupyterlab': 'jupyterlab',
    }

    # { name: [port, targetPort], .. }
    _ports = {'dask-scheduler-service': [80, 8786],
              'jupyterlab-service': [80, 8888]}

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

    def get_ports(self, servicename):
        """
        Return the port and targetport for a given server name.

        Format: [port, targetPort]

        :param servicename: service name (string),
        :return: ports (list).
        """

        return self._ports.get(servicename, 'unknown')

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

    def create_namespace(self, workdir):
        """
        Create the random namespace.

        :param workdir: path to working directory (string).
        :return: True if successful, stderr (Boolean, string).
        """

        namespace_filename = os.path.join(workdir, self._files.get('namespace', 'unknown'))
        return utilities.create_namespace(self._namespace, namespace_filename)

    def create_pvcpv(self, workdir, name='pvc'):
        """
        Create the PVC or PV.

        :param workdir: path to working directory (string).
        :param name: 'pvc' or 'pv' (string).
        :return: True if successful (Boolean), stderr (string).
        """

        if name not in ['pvc', 'pv']:
            stderr = 'unknown PVC/PC name: %s', name
            logger.warning(stderr)
            return False, stderr

        # create the yaml file
        path = os.path.join(os.path.join(workdir, self._files.get(name, 'unknown')))
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

    def deploy_service_pod(self, name, workdir):
        """
        Deploy the dask scheduler.

        :param name: service name (string).
        :param workdir: path to working directory (string).
        :return: stderr (string).
        """

        fname = self._files.get(name, 'unknown')
        if fname == 'unknown':
            stderr = 'unknown file name for %s yaml' % name
            logger.warning(stderr)
            return stderr
        image = self._images.get(name, 'unknown')
        if image == 'unknown':
            stderr = 'unknown image for %s pod' % name
            logger.warning(stderr)
            return stderr

        # create yaml
        name += '-service'
        func = utilities.get_scheduler_yaml if name == 'dask-scheduler-service' else utilities.get_jupyterlab_yaml
        path = os.path.join(workdir, fname)
        yaml = func(image_source=image,
                    nfs_path=self._mountpath,
                    namespace=self._namespace,
                    user_id=self._userid,
                    port=self.get_ports(name)[1])
        status = utilities.write_file(path, yaml, mute=False)
        if not status:
            stderr = 'cannot continue since file %s could not be created' % path
            logger.warning(stderr)
            return stderr

        # start the dask scheduler pod
        status, _, stderr = utilities.kubectl_create(filename=path)
        if not status:
            return stderr

        return ""

    def get_service_info(self, service):
        """
        Return the relevant IP and pod name for the given service (when available).

        :param service: service name (string).
        :return: IP number (string), pod name (string), stderr (string).
        """

        func = utilities.get_scheduler_info if service == 'dask-scheduler' else utilities.get_jupyterlab_info
        return func(namespace=self._namespace)

    def deploy_dask_workers(self, workdir, scheduler_ip='', scheduler_pod_name='', jupyter_pod_name=''):
        """
        Deploy all dask workers.

        :param workdir: path to working directory (string).
        :param scheduler_ip: dask scheduler IP (string).
        :param scheduler_pod_name: pod name for scheduler (string).
        :param optional jupyter_pod_name: pod name for jupyterlab (string).
        :return: True if successful, stderr (Boolean, string)
        """

        worker_info, stderr = utilities.deploy_workers(scheduler_ip,
                                                       self._nworkers,
                                                       self._files,
                                                       self._namespace,
                                                       self._userid,
                                                       self._images.get('dask-worker', 'unknown'),
                                                       self._mountpath,
                                                       workdir)
        if not worker_info:
            logger.warning('failed to deploy workers: %s', stderr)
            return False, stderr

        # wait for the worker pods to start
        # (send any scheduler and jupyter pod name to function so they can be removed from a query)
        try:
            status = utilities.await_worker_deployment(worker_info,
                                                       self._namespace,
                                                       scheduler_pod_name=scheduler_pod_name,
                                                       jupyter_pod_name=jupyter_pod_name)
        except Exception as exc:
            stderr = 'caught exception: %s', exc
            logger.warning(stderr)
            status = False

        return status, stderr

    def deploy_pilot(self, workdir, scheduler_ip):
        """
        Deploy the pilot pod.

        :param workdir: path to working directory (string).
        :param scheduler_ip: dash scheduler IP (string).
        :return: True if successful (Boolean), [None], stderr (string).
        """

        # create pilot yaml
        path = os.path.join(workdir, self._files.get('dask-pilot', 'unknown'))
        yaml = utilities.get_pilot_yaml(image_source=self._images.get('dask-pilot', 'unknown'),
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

        return utilities.wait_until_deployment(name=self._podnames.get('dask-pilot', 'unknown'), state='Running', namespace=self._namespace)

    def copy_bundle(self):
        """
        Copy bundle (incl. job definition).

        :return: True if successful (Boolean).
        """

        status = True

        return status

    def get_service_name(self, name):
        """
        Return the proper internal service name.

        :param name: general service name (string).
        :return: internal service name (string).
        """

        return self._podnames.get(name, 'unknown')

    def create_service(self, servicename, port, targetport, workdir):
        """
        Create a service yaml and start it.

        :param servicename: service name (string).
        :param port: port (int).
        :param targetport: targetport (int).
        :param workdir: path to working directory (string).
        :return: stderr (string)
        """

        _stderr = ''

        path = os.path.join(workdir, self._files.get(servicename, 'unknown'))
        yaml = utilities.get_service_yaml(namespace=self._namespace,
                                          name=self._podnames.get(servicename, 'unknown'),
                                          port=port,
                                          targetport=targetport)
        status = utilities.write_file(path, yaml, mute=False)
        if not status:
            _stderr = 'cannot continue since %s service yaml file could not be created' % servicename
            logger.warning(_stderr)
            return _stderr

        # start the service
        status, _, _stderr = utilities.kubectl_create(filename=path)
        if not status:
            logger.warning('failed to create %s pod: %s', self._podnames.get(servicename, 'unknown'), _stderr)
            return _stderr
        else:
            logger.debug('created %s service', self._podnames.get(servicename, 'unknown'))

#        status, _external_ip, _stderr = utilities.wait_until_deployment(name=self._podnames.get('dask-scheduler-service','unknown'), namespace=self._namespace)

        return _stderr

    def wait_for_service(self, name):
        """
        Wait for a given service deployment to start.

        :param name: service name (string)
        :return: host IP (string), stderr (string).
        """

        _, _ip, _stderr = utilities.wait_until_deployment(name=self._podnames.get(name, 'unknown'),
                                                          namespace=self._namespace)
        return _ip, _stderr

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
        cmd = 'kubectl delete --all deployments --namespace=%s' % namespace
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

        cmd = 'kubectl delete --all pods --namespace=%s' % namespace
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

        cmd = 'kubectl delete --all services --namespace=%s' % namespace
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

    # this should be an input parameter
    workdir = os.getcwd()

    # create unique name space
    status, stderr = submitter.create_namespace(workdir)
    if not status:
        logger.warning('failed to create namespace %s: %s', submitter.get_namespace(), stderr)
        cleanup()
        exit(-1)
    logger.info('created namespace: %s', submitter.get_namespace())

    # create PVC and PV
    for name in ['pvc', 'pv']:
        status, stderr = submitter.create_pvcpv(workdir, name=name)
        if not status:
            logger.warning('could not create PVC/PV: %s', stderr)
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid())
            exit(-1)
    logger.info('created PVC and PV')

    # create the dask scheduler service with a load balancer (the external IP of the load balancer will be made
    # available to the caller)
    # [wait until external IP is available]
    services = ['dask-scheduler', 'jupyterlab']
    for service in services:
        _service = service + '-service'
        ports = submitter.get_ports(_service)
        stderr = submitter.create_service(_service, ports[0], ports[1], workdir)
        if stderr:
            logger.warning('failed to deploy %s: %s', _service, stderr)
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
            exit(-1)

    # dictionary with service info partially to be returned to the user (external and internal IPs)
    service_info = {}  # { service: {'external_ip': <ext. ip>, 'internal_ip': <int. ip>, 'pod_name': <pod_name>}, ..}

    # start services with load balancers
    for service in services:
        _service = service + '-service'
        _ip, stderr = submitter.wait_for_service(_service)
        if stderr:
            logger.warning('failed to start load balancer for %s: %s', _service, stderr)
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
            exit(-1)
        if service not in service_info:
            service_info[service] = {}
        service_info[service]['external_ip'] = _ip

        logger.info('load balancer for %s has external ip=%s', _service, service_info[service].get('external_ip'))

    # deploy the dask scheduler (the scheduler IP will only be available from within the cluster)
    for service in services:
        stderr = submitter.deploy_service_pod(service, workdir)
        if stderr:
            logger.warning('failed to deploy %s pod: %s', service, stderr)
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
            exit(-1)
        else:
            logger.debug('deployed %s pod', service)

    # get the scheduler and jupyterlab info
    # for the dask scheduler, the internal IP number is needed
    # for jupyterlab, we only need to verify that it started properly
    for service in services:
        internal_ip, _pod_name, stderr = submitter.get_service_info(service)
        if stderr:
            logger.warning('%s pod failed: %s', service, stderr)
            #cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
            exit(-1)
        service_info[service]['internal_ip'] = internal_ip
        service_info[service]['pod_name'] = _pod_name
        if internal_ip:
            logger.info('pod %s with internal ip=%s started correctly', _pod_name, internal_ip)
        else:
            logger.info('pod %s started correctly', _pod_name)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace=namespace)

    # switch context for the new namespace
    #status = utilities.kubectl_execute(cmd='config use-context', namespace='default')

    # deploy the worker pods
    logger.debug('service_info=%s', str(service_info))

    status, stderr = submitter.deploy_dask_workers(workdir, scheduler_ip=service_info['dask-scheduler'].get('internal_ip'),
                                                   scheduler_pod_name=service_info['dask-scheduler'].get('pod_name'),
                                                   jupyter_pod_name=service_info['jupyterlab'].get('pod_name'))
    if not status:
        logger.warning('failed to deploy dask workers: %s', stderr)
        cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
        exit(-1)
    logger.info('deployed all dask-worker pods')

    info = '\n********************************************************'
    info += '\ndask scheduler has external ip=%s' % service_info['dask-scheduler'].get('external_ip')
    info += '\ndask scheduler has internal ip=%s' % service_info['dask-scheduler'].get('internal_ip')
    info += '\njupyterlab has external ip=%s' % service_info['jupyterlab'].get('external_ip')
    info += '\n********************************************************'
    logger.info(info)

    # cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
    exit(0)

    #######################################

    # deploy the pilot pod
    status, _, stderr = submitter.deploy_pilot(workdir, service_info['dask-scheduler-service'].get('internal_ip'))

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
