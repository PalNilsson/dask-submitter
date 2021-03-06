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

import utilities

logger = logging.getLogger(__name__)
ERROR_NAMESPACE = 1
ERROR_PVPVC = 2
ERROR_CREATESERVICE = 3
ERROR_LOADBALANCER = 4
ERROR_DEPLOYMENT = 5
ERROR_PODFAILURE = 6
ERROR_DASKWORKER = 7


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
    _password = None
    _interactive_mode = True
    _workdir = ''
    _nfs_server = "10.226.152.66"

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
        'dask-scheduler': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/dask-scheduler:latest',
        'dask-worker': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/dask-worker:latest',
        'dask-pilot': 'palnilsson/dask-pilot:latest',
        'jupyterlab': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/datascience-notebook:latest',
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
        self._password = kwargs.get('password', None)
        self._workdir = kwargs.get('workdir', os.getcwd())
        self._interactive_mode = kwargs.get('interactive_mode', True)

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

    def create_namespace(self):
        """
        Create the random namespace.

        :return: True if successful, stderr (Boolean, string).
        """

        namespace_filename = os.path.join(self._workdir, self._files.get('namespace', 'unknown'))
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
        path = os.path.join(os.path.join(self._workdir, self._files.get(name, 'unknown')))
        func = utilities.get_pvc_yaml if name == 'pvc' else utilities.get_pv_yaml
        yaml = func(namespace=self._namespace, user_id=self._userid, nfs_server=self._nfs_server)
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

    def deploy_service_pod(self, name):
        """
        Deploy the dask scheduler.

        :param name: service name (string).
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
        path = os.path.join(self._workdir, fname)
        yaml = func(image_source=image,
                    nfs_path=self._mountpath,
                    namespace=self._namespace,
                    user_id=self._userid,
                    port=self.get_ports(name)[1],
                    password=self._password)
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

    def deploy_dask_workers(self, scheduler_ip='', scheduler_pod_name='', jupyter_pod_name=''):
        """
        Deploy all dask workers.

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
                                                       self._workdir)
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

    def deploy_pilot(self, scheduler_ip):
        """
        Deploy the pilot pod.

        :param scheduler_ip: dash scheduler IP (string).
        :return: True if successful (Boolean), [None], stderr (string).
        """

        # create pilot yaml
        path = os.path.join(self._workdir, self._files.get('dask-pilot', 'unknown'))
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

    def create_service(self, servicename, port, targetport):
        """
        Create a service yaml and start it.

        :param servicename: service name (string).
        :param port: port (int).
        :param targetport: targetport (int).
        :return: stderr (string)
        """

        _stderr = ''

        path = os.path.join(self._workdir, self._files.get(servicename, 'unknown'))
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

#        status, _external_ip, _stderr = utilities.wait_until_deployment(name=self._podnames.get('dask-scheduler-service','unknown'), namespace=self._namespace)

        return _stderr

    def wait_for_service(self, name):
        """
        Wait for a given service deployment to start.

        :param name: service name (string)
        :return: host IP (string), stderr (string).
        """

        _, _ip, _stderr = utilities.wait_until_deployment(name=self._podnames.get(name, 'unknown'),
                                                          namespace=self._namespace, service=True)
        return _ip, _stderr

    def install(self, timing):
        """
        Install all services and deploy all pods.

        The service_info dictionary containers service info partially to be returned to the user (external and internal IPs)
        It has the format
          { service: {'external_ip': <ext. ip>, 'internal_ip': <int. ip>, 'pod_name': <pod_name>}, ..}

        :param timing: timing dictionary.
        :return: exit code (int), service_info (dictionary), stderr (string).
        """

        exitcode = 0
        service_info = {}

        # create unique name space
        status, stderr = submitter.create_namespace()
        if not status:
            stderr = 'failed to create namespace %s: %s' % (submitter.get_namespace(), stderr)
            logger.warning(stderr)
            cleanup()
            return ERROR_NAMESPACE, {}, stderr
        timing['tnamespace'] = time.time()
        logger.info('created namespace: %s', submitter.get_namespace())

        # create PVC and PV
        for name in ['pvc', 'pv']:
            status, stderr = submitter.create_pvcpv(name=name)
            if not status:
                stderr = 'could not create PVC/PV: %s' % stderr
                logger.warning(stderr)
                cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid())
                exitcode = ERROR_PVPVC
                break
        timing['tpvcpv'] = time.time()
        if exitcode:
            return exitcode, {}, stderr
        logger.info('created PVC and PV')

        # create the dask scheduler service with a load balancer (the external IP of the load balancer will be made
        # available to the caller)
        # [wait until external IP is available]
        services = ['dask-scheduler', 'jupyterlab']
        for service in services:
            _service = service + '-service'
            ports = submitter.get_ports(_service)
            stderr = submitter.create_service(_service, ports[0], ports[1])
            if stderr:
                exitcode = ERROR_CREATESERVICE
                cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
                break
        timing['tservices'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # start services with load balancers
        for service in services:
            _service = service + '-service'
            _ip, stderr = submitter.wait_for_service(_service)
            if stderr:
                stderr = 'failed to start load balancer for %s: %s' % (_service, stderr)
                logger.warning(stderr)
                cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
                exitcode = ERROR_LOADBALANCER
                break
            if service not in service_info:
                service_info[service] = {}
            service_info[service]['external_ip'] = _ip
            logger.info('load balancer for %s has external ip=%s', _service, service_info[service].get('external_ip'))
        timing['tloadbalancers'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # deploy the dask scheduler (the scheduler IP will only be available from within the cluster)
        for service in services:
            stderr = submitter.deploy_service_pod(service)
            if stderr:
                stderr = 'failed to deploy %s pod: %s' % (service, stderr)
                logger.warning(stderr)
                cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
                exitcode = ERROR_DEPLOYMENT
                break
        timing['tdeployments'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # get the scheduler and jupyterlab info
        # for the dask scheduler, the internal IP number is needed
        # for jupyterlab, we only need to verify that it started properly
        for service in services:
            internal_ip, _pod_name, stderr = submitter.get_service_info(service)
            if stderr:
                stderr = '%s pod failed: %s' % (service, stderr)
                logger.warning(stderr)
                cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
                exitcode = ERROR_PODFAILURE
                break
            service_info[service]['internal_ip'] = internal_ip
            service_info[service]['pod_name'] = _pod_name
            if internal_ip:
                logger.info('pod %s with internal ip=%s started correctly', _pod_name, internal_ip)
            else:
                logger.info('pod %s started correctly', _pod_name)
        timing['tserviceinfo'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # switch context for the new namespace
        # status = utilities.kubectl_execute(cmd='config use-context', namespace=namespace)

        # switch context for the new namespace
        # status = utilities.kubectl_execute(cmd='config use-context', namespace='default')

        # deploy the worker pods
        status, stderr = submitter.deploy_dask_workers(scheduler_ip=service_info['dask-scheduler'].get('internal_ip'),
                                                       scheduler_pod_name=service_info['dask-scheduler'].get('pod_name'),
                                                       jupyter_pod_name=service_info['jupyterlab'].get('pod_name'))
        if not status:
            stderr = 'failed to deploy dask workers: %s' % stderr
            logger.warning(stderr)
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
            exitcode = ERROR_DASKWORKER
        timing['tdaskworkers'] = time.time()
        if exitcode:
            return exitcode, {}, stderr
        logger.info('deployed all dask-worker pods')

        # launch pilot pod with stager workflow enabled
        # note: the pilot will start staging files asynchronously to everything else. The user will need to check
        # when the input files are available (a staging report can be made available) in the user area

        # need to communicate user area + job description
        # this script should place the job description in the user area

        # return the jupyterlab and dask scheduler IPs to the user in interactive mode
        if self._interactive_mode:
            return exitcode, service_info, stderr

        #######################################

        # deploy the pilot pod
        status, _, stderr = submitter.deploy_pilot(service_info['dask-scheduler-service'].get('internal_ip'))

        # time.sleep(30)
        cmd = 'kubectl logs dask-pilot --namespace=%s' % submitter.get_namespace()
        logger.debug('executing: %s', cmd)
        ec, stdout, stderr = utilities.execute(cmd)
        logger.debug(stdout)

        if not status:
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
            exit(-1)
        logger.info('deployed pilot pod')

        return exitcode, stderr

    def timing_report(self, timing, info=None):
        """
        Display the timing report.

        :param timing: timing dictionary.
        :param info: info string to be prepended to timing report (string).
        :return:
        """

        _info = info if info else ''
        _info += '\n* timing report ****************************************'
        for key in timing:
            _info += '\n%s:\t\t\t%d s' % (key, timing.get(key) - timing.get('t0'))
        _info += '\n----------------------------------'
        _info += '\ntotal time:\t\t\t%d s' % sum((timing[key] - timing['t0']) for key in timing)
        _info += '\n********************************************************'
        logger.info(_info)

    def create_cleanup_script(self):
        """
        Create a clean-up script, useful for interactive sessions (at least in stand-alone mode).

        :return:
        """

        cmds = '#!/bin/bash\n'
        cmds += 'kubectl delete --all deployments --namespace=single-user-%s\n' % self._userid
        cmds += 'kubectl delete --all pods --namespace=single-user-%s\n' % self._userid
        cmds += 'kubectl delete --all services --namespace=single-user-%s\n' % self._userid
        cmds += 'kubectl patch pvc fileserver-claim -p \'{"metadata":{"finalizers":null}}\' --namespace=single-user-%s\n' % self._userid
        cmds += 'kubectl delete pvc fileserver-claim --namespace=single-user-%s\n' % self._userid
        cmds += 'kubectl patch pv fileserver-%s -p \'{"metadata":{"finalizers":null}}\' --namespace=single-user-%s\n' % (self._userid, self._userid)
        cmds += 'kubectl delete pv fileserver-%s --namespace=single-user-%s\n' % (self._userid, self._userid)
        cmds += 'kubectl delete namespaces single-user-%s\n' % self._userid

        path = os.path.join(self._workdir, 'deleteall.sh')
        status = utilities.write_file(path, cmds)
        if not status:
            return False, 'write_file failed for file %s' % path
        else:
            os.chmod(path, 0o755)

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


if __name__ == '__main__':

    timing = {'t0': time.time()}
    utilities.establish_logging()

    logging.info("*** Dask submitter ***")
    logging.info("Python version %s", sys.version)

    # input parameters [to be passed to the script]
    workdir = os.getcwd()  # working directory
    nworkers = 2  # number of dask workers
    interactive_mode = True  # True means interactive jupyterlab session, False means pilot pod runs user payload
    password = 'trustno1'  # jupyterlab password
    userid = ''.join(random.choice(ascii_lowercase) for _ in range(5))  # unique 5-char user id (basically for K8)

    submitter = DaskSubmitter(nworkers=nworkers,
                              password=password,
                              interactive_mode=interactive_mode,
                              workdir=workdir,
                              userid=userid)
    try:
        exitcode, service_info, diagnostics = submitter.install(timing)
        if exitcode:
            exit(-1)
        if service_info:
            info = '\n********************************************************'
            info += '\nuser id: %s' % userid
            info += '\ndask scheduler has external ip %s' % service_info['dask-scheduler'].get('external_ip')
            info += '\ndask scheduler has internal ip %s' % service_info['dask-scheduler'].get('internal_ip')
            info += '\njupyterlab has external ip %s' % service_info['jupyterlab'].get('external_ip')

        # done, cleanup and exit
        if interactive_mode:
            submitter.create_cleanup_script()
        else:
            cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
    except Exception as exc:
        logger.warning('exception caught: %s', exc)
        cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)

    submitter.timing_report(timing, info=info)
    exit(0)
