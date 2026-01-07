import logging
import operator
import os
import uuid
from datetime import datetime

from .auth import DEFAULT_USER

STATUS_DASK_TO_API = {
    "pending": "in_progress",
    "finished": "completed",
    "error": "failed",
    "lost": "failed",
    "cancelled": "in_progress",  # the cancelled status can occur when the order is re-submitted
}

logger = logging.getLogger(__name__)


class TransformationOrder(object):
    __slots__ = (
        "_client",
        "_future",
        "_order_id",
        "_info",
        "_task_parameters",
        "_uri_root",
        "_output_product_path",
        "_task_id",
    )

    def __init__(
        self,
        client,
        order_id,
        product_reference,
        workflow_id,
        workflow_options,
        workflow_name="",
        enable_monitoring=True,
        monitoring_polling_time_s=10,
        uri_root="",
    ):
        self._client = client
        self._order_id = order_id
        self._uri_root = uri_root
        self._output_product_path = ""
        self._future = None
        self._task_id = order_id

        self._task_parameters = {
            "order_id": order_id,
            "product_reference": product_reference,
            "workflow_id": workflow_id,
            "workflow_options": workflow_options,
            "enable_monitoring": enable_monitoring,
            "monitoring_polling_time_s": monitoring_polling_time_s,
        }

        self._info = {
            "Id": order_id,
            "InputProductReference": product_reference,
            "WorkflowOptions": workflow_options,
            "WorkflowId": workflow_id,
            "WorkflowName": workflow_name,
        }

    def submit(self, id_suffix=None):
        # definition of the task must be internal
        # to avoid dask to import esa_tf_restapi in the workers

        def task(**kwargs):
            import esa_tf_platform

            return esa_tf_platform.run_workflow(**kwargs)

        if id_suffix is not None:
            self._task_id = self._task_parameters["order_id"] + "-" + id_suffix
        self._future = self._client.submit(
            task, **self._task_parameters, key=self._task_id
        )
        self._info["SubmissionDate"] = datetime.now().isoformat()
        self.update_status()
        self._future.add_done_callback(self.add_completed_info)

    def resubmit(self):
        if self.get_status == "failed":
            self.client.retry(self.future)
        else:
            self._future = None
            self.clean_completed_info()
            self.submit(id_suffix=uuid.uuid4().hex)

    def maybe_resubmit(self):
        status = self.get_status()
        order_id = self._task_parameters["order_id"]
        logger.info(f"oder {order_id!r} status is {status!r}")

        if status == "completed":
            output_dir = os.getenv("OUTPUT_DIR", "./output_dir")
            full_output_path = os.path.join(output_dir, self._output_product_path)
            output_path_exist = os.path.exists(full_output_path)
            if not output_path_exist:
                logger.info(
                    f"oder {order_id!r} output product {full_output_path!r} not found: "
                    f"re-submitting order {order_id!r}"
                )
                self.resubmit()
        elif status == "failed":
            logger.info(f"re-submitting order {order_id!r}")
            self.resubmit()

    def update_output_product_reference(self):
        basepath, reference = os.path.split(self._output_product_path)
        uri_root = self._uri_root or ""
        self._info["OutputProductReference"] = [
            {
                "Reference": reference,
                "DownloadURI": f"{uri_root}download/{basepath}/{reference}",
            }
        ]

    def add_completed_info(self, future=None):
        if self._future.status == "cancelled":
            self.clean_completed_info()
        else:
            self.update_status()
            status = self.get_status()

            if status in ("completed", "failed"):
                self._info["CompletedDate"] = datetime.now().isoformat()
            if status == "completed":
                self._output_product_path = self._future.result()
                self.update_output_product_reference()

    def clean_completed_info(self):
        self._info.pop("Status", None)
        self._info.pop("CompletedDate", None)
        self._info.pop("OutputProductReference", None)
        self._output_product_path = ""

    def get_info(self):
        self.update_status()
        if self.get_status() == "completed":
            self.update_output_product_reference()
        return self._info

    def get_log(self):
        seconds_logs = self._client.get_events(self._future.key)
        logs = []
        for seconds, log in seconds_logs:
            logs.append(log)
        return logs

    def update_status(self):
        future_status = self._future.status
        self._info["Status"] = STATUS_DASK_TO_API.get(future_status, future_status)

    def get_status(self):
        self.update_status()
        return self._info["Status"]

    def get_dask_orders_status(self):
        def orders_status_on_scheduler(dask_scheduler):
            return {
                task_id: task.state for task_id, task in dask_scheduler.tasks.items()
            }

        return self._client.run_on_scheduler(orders_status_on_scheduler)


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

        valid_orders = {}
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
                valid_orders[order_id] = order

        return valid_orders
