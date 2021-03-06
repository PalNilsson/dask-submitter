# dask-submitter

Code that launches a dask scheduler, worker(s) and a pilot script for executing user scripts on a dask cluster. 
It serves as a template for later Harvester integration.

### Preliminaries

Google Cloud commands (kubectl) are expected be available.

### Operations

The script performs the following operations

1. Creation of unique namespace
2. Creation of PVC and PV for the new namespace
3. Creation of a dask scheduler service with a load balancer
4. Creation of a jupyterlab service with a load balancer
5. Start-up of dask scheduler deployment using external image
6. Start-up of jupyterlab deployment using external image
5. Start-up of required number of dask workers using external image
6. Start-up of pilot pod using external image

The name of the namespace is "single-user-<user id>", where the user id is
a five char long random letter string, used by all relevant operations.
E.g. the name of the file server is "fileserver-<user id>", i.e. only used
by the pods for the particular single-user.

The script grabs the scheduler id when it is available and forwards it to
the worker and pilot pods upon their start-ups using an environmental
variable set in the corresponding yaml file.

The pilot pod downloads the user dask-script from a known location and
executes it after connecting to the dask scheduler (i.e. creating a client)
and making sure that enough (?) workers are running.

### Comments

Refactoring is in progress, since currently everything is done in the
__ main __() function.
