import os

import dask.distributed

CLIENT = None
TRANSFORMATION_ORDERS = {}


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


def build_order_status(order):
    future = order["future"]
    order_status = {
        "Id": future.key,
        "Status": future.status,
        "WorkflowId": order["workflow_id"],
        "InputProductReference": order["input_product_reference"],
        "WorkflowOptions": order["workflow_options"],
    }
    if future.status == "finished":
        order_status["OutputFile"] = os.path.basename(future.result())

    return order_status


def get_order_status(order_id):
    order = TRANSFORMATION_ORDERS.get(order_id, None)
    if order is None:
        raise ValueError(f"Transformation Order {order_id} not found")
    order_status = build_order_status(order)
    return order_status


def get_transformation_orders(workflow_id=None, status=None):
    orders_status = []
    for order in TRANSFORMATION_ORDERS.values():
        add_order = (not workflow_id or (workflow_id == order["workflow_id"])) and (
            not status or (status == order["future"].status)
        )
        if add_order:
            order_status = build_order_status(order)
            orders_status.append(order_status)
    return orders_status


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

    client = instantiate_client(scheduler)
    future = client.submit(task, key=order_id)
    TRANSFORMATION_ORDERS[future.key] = {
        "future": future,
        "input_product_reference": input_product_reference,
        "workflow_options": workflow_options,
        "workflow_id": workflow_id,
    }
    return future.key
