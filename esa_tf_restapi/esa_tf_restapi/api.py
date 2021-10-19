import os

import dask.distributed

CLIENT = None


def instantiate_client(dask_scheduler=None):
    if dask_scheduler is None:
        dask_scheduler = os.getenv("SCHEDULER")
    if dask_scheduler is None:
        raise ValueError("Scheduler not defined")
    global CLIENT
    if CLIENT is None:
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
    status = client.cluster.scheduler.get_task_status(keys=[order_id])[order_id]
    return status


def submit_workflow(workflow_id, *, product, workflow_kwargs, scheduler=None):
    """Submit a workflow  on a product and returns the order_id
    """

    def task():
        import esa_tf_platform

        return esa_tf_platform.run(
            workflow_id, workflow_kwargs=workflow_kwargs, product=product,
        )

    client = instantiate_client(scheduler)
    future = client.submit(task)
    return future.key
