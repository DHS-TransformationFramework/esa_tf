import os
import uuid

import dask.distributed

CLIENT = dask.distributed.Client("tcp://192.168.1.117:8786")


def instantiate_client(dask_scheduler=None):
    global CLIENT
    if not CLIENT:
        if dask_scheduler is None:
            dask_scheduler = os.getenv("SCHEDULER")
        if dask_scheduler is None:
            raise ValueError("Scheduler not defined")
        CLIENT = dask.distributed.Client(dask_scheduler)
    return CLIENT


def get_workflow_by_id(workflow_id, scheduler=None):
    def task():
        import esa_tf_platform

        return esa_tf_platform.get_workflow_by_id(workflow_id)

    client = instantiate_client(scheduler)
    future = client.submit(task, priority=10)
    return client.gather(future)


def get_workflows(product=None, scheduler=None):
    def task():
        import esa_tf_platform

        return esa_tf_platform.get_workflows(product)

    client = instantiate_client(scheduler)
    future = client.submit(task, priority=10)
    return client.gather(future)


def get_order_status(order_id, scheduler=None):
    client = instantiate_client(scheduler)
    status = client.futures[order_id]
    return status


def submit_workflow(
    workflow_id,
    *,
    product_reference,
    workflow_options,
    working_dir=None,
    output_dir=None,
    hubs_credentials_file=None,
    scheduler=None,
):
    """
    Submit the workflow defined by 'workflow_id' using dask:
    :param str workflow_id:  id that identifies the workflow to run
    :param dict product_reference: dictionary containing the information to retrieve the product to be processed
    ('Reference', i.e. product name and 'api_hub', i.e. name of the ub where to download the data), e.g.:
    {'Reference': 'S2A_MSIL1C_20170205T105221_N0204_R051_T31TCF_20170205T105426', 'api_hub', 'scihub'}.
    :param dict workflow_options: dictionary cotaining the workflow kwargs.
    :param str working_dir: optional working directory where will be create the processing directory. If it is None
    it is used the value of the environment variable "WORKING_DIR".
    :param str output_dir: optional output directory. If it is None it is used the value of the environment
    variable "OUTPUT_DIR"
    :param str hubs_credential_file:  optional file containing the credential of the hub. If it is None it
    is used the value of the environment variable "HUBS_CREDENTIALS_FILE"
    :param str scheduler:  optional the scheduler to be used fot the client instantiation. If it is None it will be used
    the value of environment variable SCHEDULER.
    """
    order_id = str(uuid.uuid4())

    def task():
        import esa_tf_platform

        return esa_tf_platform.run_workflow(
            workflow_id,
            product_reference=product_reference,
            workflow_options=workflow_options,
            order_id=order_id,
            working_dir=working_dir,
            output_dir=output_dir,
            hubs_credentials_file=hubs_credentials_file,
        )

    client = instantiate_client(scheduler)
    future = client.submit(task, key=order_id)
    FUTURES[future.key] = future
    return future.key


FUTURES = {}
