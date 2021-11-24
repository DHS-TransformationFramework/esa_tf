import copy
import os
from datetime import datetime

import dask.distributed

CLIENT = None
TRANSFORMATION_ORDERS = {}


STATUS_DASK_TO_API = {
    "pending": "in_progress",
    "finished": "completed",
    "error": "failed",
}


def instantiate_client(scheduler_addr=None):
    """
    Return a client with a scheduler with address ``scheduler_addr``.
    """
    global CLIENT

    if scheduler_addr is None:
        scheduler_addr = os.getenv("SCHEDULER")
    if scheduler_addr is None:
        raise ValueError("Scheduler not defined")

    if not CLIENT or CLIENT.scheduler.addr != scheduler_addr:
        CLIENT = dask.distributed.Client(scheduler_addr)

    return CLIENT


def get_workflow_by_id(workflow_id, scheduler=None):
    # definition of the task must be internal
    # to avoid dask to import esa_tf_restapi in the workers
    def task():
        import esa_tf_platform

        return esa_tf_platform.get_workflow_by_id(workflow_id)

    client = instantiate_client(scheduler)
    future = client.submit(task, priority=10)
    return client.gather(future)


def get_workflows(product=None, scheduler=None):
    # definition of the task must be internal
    # to avoid dask to import esa_tf_restapi in the workers
    def task():
        import esa_tf_platform

        return esa_tf_platform.get_workflows(product)

    client = instantiate_client(scheduler)
    future = client.submit(task, priority=10)
    return client.gather(future)


def build_transformation_order(order):
    transformation_order = copy.deepcopy(order)
    transformation_order.pop("future")
    future = order["future"]
    transformation_order["Status"] = STATUS_DASK_TO_API[future.status]

    if future.status == "finished":
        transformation_order["OutputFile"] = os.path.basename(future.result())

    return transformation_order


def get_transformation_order(order_id):
    order = TRANSFORMATION_ORDERS.get(order_id, None)
    if order is None:
        raise KeyError(f"Transformation Order {order_id} not found")
    transformation_order = build_transformation_order(order)
    return transformation_order


def get_transformation_orders(status=None, workflow_id=None):
    transformation_orders = []
    for order in TRANSFORMATION_ORDERS.values():
        transformation_order = build_transformation_order(order)
        add_order = (not workflow_id or (workflow_id == order["WorkflowId"])) and (
            not status or (status == transformation_order["Status"])
        )
        if add_order:
            transformation_orders.append(transformation_order)
    return transformation_orders


def submit_workflow(
    workflow_id,
    *,
    input_product_reference,
    workflow_options,
    working_dir=None,
    output_dir=None,
    hubs_credentials_file=None,
    scheduler=None,
    order_id=None,
):
    """
    Submit the workflow defined by 'workflow_id' using dask:
    :param str workflow_id:  id that identifies the workflow to run
    :param dict input_product_reference: dictionary containing the information to retrieve the product to be processed
    ('Reference', i.e. product name and 'api_hub', i.e. name of the ub where to download the data), e.g.:
    {'Reference': 'S2A_MSIL1C_20170205T105221_N0204_R051_T31TCF_20170205T105426', 'api_hub', 'scihub'}.
    :param dict workflow_options: dictionary cotaining the workflow kwargs.
    :param str working_dir: optional working directory where will be create the processing directory. If it is None
    it is used the value of the environment variable "WORKING_DIR".
    :param str output_dir: optional output directory. If it is None it is used the value of the environment
    variable "OUTPUT_DIR"
    :param str hubs_credentials_file:  optional file containing the credential of the hub. If it is None it
    is used the value of the environment variable "HUBS_CREDENTIALS_FILE"
    :param str scheduler:  optional the scheduler to be used fot the client instantiation. If it is None it will be used
    the value of environment variable SCHEDULER.
    """
    if not order_id:
        order_id = dask.base.tokenize(
            workflow_id, input_product_reference, workflow_options,
        )

    # definition of the task must be internal
    # to avoid dask to import esa_tf_restapi in the workers
    def task():
        import esa_tf_platform

        return esa_tf_platform.run_workflow(
            workflow_id,
            product_reference=input_product_reference,
            workflow_options=workflow_options,
            order_id=order_id,
            working_dir=working_dir,
            output_dir=output_dir,
            hubs_credentials_file=hubs_credentials_file,
        )

    workflow_name = get_workflows()[workflow_id]["WorkflowName"]
    client = instantiate_client(scheduler)
    future = client.submit(task, key=order_id)
    current_time = datetime.now()
    TRANSFORMATION_ORDERS[future.key] = {
        "future": future,
        "Id": future.key,
        "SubmissionDate": current_time.strftime("%Y-%m-%dT%H:%m:%S"),
        "InputProductReference": input_product_reference,
        "WorkflowOptions": workflow_options,
        "WorkflowId": workflow_id,
        "WorkflowName": workflow_name,
    }
    return build_transformation_order(TRANSFORMATION_ORDERS[future.key])
