import logging
import operator
import os
from datetime import datetime

from .auth import DEFAULT_USER

STATUS_DASK_TO_API = {
    "pending": "in_progress",
    "finished": "completed",
    "error": "failed",
}

logger = logging.getLogger(__name__)


class TransformationOrder(object):
    __slots__ = ("future", "parameters", "uri_root", "_info", "client")

    @classmethod
    def submit(
        cls,
        client,
        order_id,
        product_reference,
        workflow_id,
        workflow_options,
        workflow_name=None,
        uri_root=None,
    ):
        parameters = {
            "order_id": order_id,
            "product_reference": product_reference,
            "workflow_id": workflow_id,
            "workflow_options": workflow_options,
        }
        # definition of the task must be internal
        # to avoid dask to import esa_tf_restapi in the workers

        def task():
            import esa_tf_platform

            return esa_tf_platform.run_workflow(**parameters)

        future = client.submit(task, key=order_id)
        transformation_order = TransformationOrder()
        transformation_order.client = client
        transformation_order.future = future
        transformation_order.future.add_done_callback(
            transformation_order.add_completed_info
        )
        transformation_order._info = {
            "Id": order_id,
            "SubmissionDate": datetime.now().isoformat(),
            "InputProductReference": product_reference,
            "WorkflowOptions": workflow_options,
            "WorkflowId": workflow_id,
            "WorkflowName": workflow_name,
        }
        transformation_order.parameters = parameters
        transformation_order.uri_root = uri_root
        return transformation_order

    def resubmit(self, resubmit_if_failed=False):
        if not resubmit_if_failed or self.future.status == "error":
            self.clean_completed_info()
            self._info["SubmissionDate"] = datetime.now().isoformat()
            self.client.retry(self.future)
            self.future.add_done_callback(self.add_completed_info)

    def add_completed_info(self, future):
        self._info["CompletedDate"] = datetime.now().isoformat()
        self._info["Status"] = STATUS_DASK_TO_API[self.future.status]
        if self.future.status == "finished":
            basepath, reference = os.path.split(self.future.result())
            uri_root = self.uri_root or ""
            self._info["OutputProductReference"] = [
                {
                    "Reference": reference,
                    "DownloadURI": f"{uri_root}download/{reference}/{basepath}",
                }
            ]

    def clean_completed_info(self):
        self._info.pop("Status", None)
        self._info.pop("CompletedDate", None)
        self._info.pop("OutputProductReference", None)

    def get_info(self):
        self.update_status()
        return self._info

    def get_log(self):
        seconds_logs = self.client.get_events(self.future.key)
        logs = []
        for seconds, log in seconds_logs:
            logs.append(log)
        return logs

    def update_status(self):
        # Note: the future must be extracted from the original order. The deepcopy breaks the future
        self._info["Status"] = STATUS_DASK_TO_API[self.future.status]

    def get_status(self):
        self.update_status()
        return self._info["Status"]


    # @staticmethod
    # def get_dask_orders_status():
    #     def orders_status_on_scheduler(dask_scheduler):
    #         return {task_id: task.state for task_id, task in dask_scheduler.tasks.items()}
    #
    #     client = instantiate_client()
    #     return client.run_on_scheduler(orders_status_on_scheduler)


class Queue(object):
    __slots__ = ("transformation_orders", "user_to_orders", "order_to_users")

    def __init__(self):
        self.transformation_orders = {}
        self.user_to_orders = {}
        self.order_to_users = {}

    def add_order(self, transformation_order, user_id=DEFAULT_USER):
        order_id = transformation_order.get_info()["Id"]
        if order_id not in self.transformation_orders:
            self.transformation_orders[order_id] = transformation_order
        self.user_to_orders.setdefault(user_id, set()).add(order_id)
        self.order_to_users.setdefault(order_id, set()).add(user_id)

    def remove_order(self, order_id):
        self.transformation_orders.pop(order_id)
        users_ids = self.order_to_users.pop(order_id, [])
        for user_id in users_ids:
            self.user_to_orders[user_id].discard(order_id)

    def update_orders(self, orders, user_id=DEFAULT_USER):
        for order in orders:
            self.add_order(order, user_id=user_id)

    def remove_old_orders(self, keeping_period, reference_time=None):
        """Update the queue removing only the
        transformations with statuses `completed` or `failed` that are older than the `keeping_period`.
        It returns the list of order-IDs that have been deleted.

        :param int keeping_period: the minimum number of minutes from the CompletedDate that a
        TransformationOrder will be kept in memory
        :param datetime.datetime reference_time: the time w.r.t. the keeping_period is calculated
        :return list:
        """
        now = datetime.now() if reference_time is None else reference_time
        # find completed or failed orders that are older than keeping_period
        orders_to_remove = []
        for order_id, order in self.transformation_orders.items():
            completed_date = order.get_info().get("CompletedDate", None)
            if completed_date:
                elapsed_minutes = (
                    now - datetime.fromisoformat(completed_date)
                ).total_seconds() / 60  # in minutes
                if elapsed_minutes > keeping_period:
                    orders_to_remove.append(order_id)
        for order_id in orders_to_remove:
            self.remove_order(order_id)

    def get_count_uncompleted_orders(self, user_id):
        """Return the number of running processes (i.e. status equal to `in_progress`) among those
        required by the `user_id`.

        :param str user_id: user identifier
        :return int: count of uncompleted orders
        """
        running_processes = 0
        for order_id in self.user_to_orders.get(user_id, []):
            order_status = self.transformation_orders[order_id].get_status()
            running_processes += order_status in ("in_progress", "queued")
        return running_processes

    def get_transformation_orders(
        self,
        filters=[],
        user_id=DEFAULT_USER,
        filter_by_user_id=True,
    ):
        if not filter_by_user_id:
            transformation_orders = self.transformation_orders.copy()
        else:
            transformation_orders = {
                order_id: self.transformation_orders[order_id]
                for order_id in self.user_to_orders.get(user_id, [])
            }

        valid_orders_info = []
        for order_id, order in transformation_orders.items():
            order_info = self.transformation_orders[order_id].get_info()
            add_order = True
            for key, op, value in filters:
                if key == "CompletedDate" and "CompletedDate" not in order_info:
                    add_order = False
                    continue
                op = getattr(operator, op)
                if key == "InputProductReference":
                    order_value = order_info["InputProductReference"]["Reference"]
                else:
                    order_value = order_info[key]
                if key in {"CompletedDate", "SubmissionDate"}:
                    order_value = datetime.fromisoformat(order_value)
                    value = datetime.fromisoformat(value)
                add_order = add_order and op(order_value, value)
            if add_order:
                valid_orders_info.append(order_info)

        return valid_orders_info
