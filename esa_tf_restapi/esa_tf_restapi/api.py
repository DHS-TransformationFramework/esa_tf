import copy
import functools
import logging
import operator
import os
import re
import typing as T
import yaml
from datetime import datetime

import dask.distributed

logger = logging.getLogger(__name__)

CLIENT = None
TRANSFORMATION_ORDERS = {}
USERS_TRANSFORMATIONS = {}


STATUS_DASK_TO_API = {
    "pending": "in_progress",
    "finished": "completed",
    "error": "failed",
}


SENTINEL1 = [
    "S1_RAW__0S",
    "S2_RAW__0S",
    "S3_RAW__0S",
    "S4_RAW__0S",
    "S5_RAW__0S",
    "S6_RAW__0S",
    "IW_RAW__0S",
    "EW_RAW__0S",
    "WV_RAW__0S",
    "S1_SLC__1S",
    "S2_SLC__1S",
    "S3_SLC__1S",
    "S4_SLC__1S",
    "S5_SLC__1S",
    "S6_SLC__1S",
    "IW_SLC__1S",
    "EW_SLC__1S",
    "WV_SLC__1S",
    "S1_GRDH_1S",
    "S2_GRDH_1S",
    "S3_GRDH_1S",
    "S4_GRDH_1S",
    "S5_GRDH_1S",
    "S6_GRDH_1S",
    "IW_GRDH_1S",
    "EW_GRDH_1S",
    "S1_GRDM_1S",
    "S2_GRDM_1S",
    "S3_GRDM_1S",
    "S4_GRDM_1S",
    "S5_GRDM_1S",
    "S6_GRDM_1S",
    "IW_GRDM_1S",
    "EW_GRDM_1S",
    "S1_OCN__2S",
    "S2_OCN__2S",
    "S3_OCN__2S",
    "S4_OCN__2S",
    "S5_OCN__2S",
    "S6_OCN__2S",
    "IW_OCN__2S",
    "EW_OCN__2S",
    "WV_OCN__2S",
]

SENTINEL2 = ["S2MSI1C", "S2MSI2A"]


def _prepare_download_uri(output_product_referece: list, root_uri: str):
    for product in output_product_referece:
        product[
            "DownloadURI"
        ] = f"{root_uri}download/{product['ReferenceBasePath']}/{product['Reference']}"
        del product["ReferenceBasePath"]
    return output_product_referece


def add_completed_date(future):
    order = TRANSFORMATION_ORDERS[future.key]
    order["CompletedDate"] = datetime.now().isoformat()


def check_products_consistency(
    product_type, input_product_reference_name, workflow_id=None
):
    """
    Check if the workflow product type is consistent with the product name.
    The check is done on the first characters of `input_product_reference_name`.
    Currently are supported only Sentinel1 nd Sentinel2 products.
    """

    if product_type in SENTINEL1:
        exp = f"^S1[AB]_{product_type}"
    elif product_type in SENTINEL2:
        exp = f"^S2[AB]_{product_type[2:5]}L{product_type[5:7]}"
    else:
        raise ValueError(
            f"product type ${product_type} not recognized, error in plugin: {workflow_id!r}"
        )

    if not re.match(exp, str(input_product_reference_name)):
        raise ValueError(
            f"input product name {input_product_reference_name!r} does not comply "
            f"to the naming convention for the {product_type!r} product type required by "
            f"{workflow_id!r}"
        )


def instantiate_client(scheduler_addr=None):
    """
    Return a client with a scheduler with address ``scheduler_addr``.
    """
    global CLIENT

    if scheduler_addr is None:
        scheduler_addr = os.getenv("SCHEDULER")
    if scheduler_addr is None:
        raise ValueError("environment variable 'SCHEDULER' not found")
    if not CLIENT or CLIENT.scheduler.addr != scheduler_addr:
        CLIENT = dask.distributed.Client(scheduler_addr)

    return CLIENT


def filter_by_product_type(workflows, product_type=None):
    filtered_workflows = {}
    for name in workflows:
        if product_type == workflows[name]["InputProductType"]:
            filtered_workflows[name] = workflows[name]
    return filtered_workflows


@functools.lru_cache()
def get_all_workflows(scheduler=None):
    """
    Return the workflows configurations installed in the workers.
    """
    # definition of the task must be internal
    # to avoid dask to import esa_tf_restapi in the workers
    def task():
        import esa_tf_platform

        return esa_tf_platform.get_all_workflows()

    client = instantiate_client(scheduler)
    future = client.submit(task, priority=10)
    workflows = client.gather(future)
    for name in workflows:
        workflows[name].pop("Execute")
    return workflows


def get_workflow_by_id(workflow_id, scheduler=None):
    """
    Return the workflow configuration corresponding to the workflow_id.
    """
    # definition of the task must be internal
    # to avoid dask to import esa_tf_restapi in the workers
    workflows = get_all_workflows()
    try:
        workflow = workflows[workflow_id]
    except KeyError:
        raise KeyError(
            f"Workflow {workflow_id!r} not found, the available workflows are {list(workflows)!r}"
        )
    return workflow


def get_workflows(product=None, scheduler=None):
    """
    Return the workflows configurations installed in the workers.
    They may be filtered using the product type
    """
    workflows = get_all_workflows()
    if product:
        workflows = filter_by_product_type(workflows, product)
    return workflows


def build_transformation_order(order, uri_root=None):
    # Note: the future must be extracted from the original order. The deepcopy breaks the future
    future = order["future"]
    transformation_order = copy.deepcopy(order)
    transformation_order.pop("future")
    transformation_order["Status"] = STATUS_DASK_TO_API[future.status]
    if future.status == "finished":
        if not transformation_order.get("OutputProductReference", {}):
            basepath, reference = os.path.split(future.result())
            transformation_order["OutputProductReference"] = [
                {"Reference": reference, "ReferenceBasePath": basepath}
            ]
            if uri_root:
                transformation_order["OutputProductReference"] = _prepare_download_uri(
                    transformation_order["OutputProductReference"], uri_root
                )
    # if future.status == "error":
    #     transformation_order["ErrorMessage"] = str(future.exception())
    return transformation_order


def get_dask_orders_status():
    def orders_status_on_scheduler(dask_scheduler):
        return {task_id: task.state for task_id, task in dask_scheduler.tasks.items()}

    client = instantiate_client()
    return client.run_on_scheduler(orders_status_on_scheduler)


def get_transformation_order_log(order_id):
    if order_id not in TRANSFORMATION_ORDERS:
        raise KeyError(f"Transformation Order {order_id!r} not found")
    client = instantiate_client()
    seconds_logs = client.get_events(order_id)
    logs = []
    for seconds, log in seconds_logs:
        logs.append(log)
    return logs


def get_transformation_order(order_id, uri_root=None):
    """
    Return the transformation order corresponding to the order_id
    """
    order = TRANSFORMATION_ORDERS.get(order_id)
    if order is None:
        raise KeyError(f"Transformation Order {order_id!r} not found")
    transformation_order = build_transformation_order(order, uri_root=uri_root)
    return transformation_order


def check_filter_validity(filters):
    allowed_filters = {
        "Id": ("eq",),
        "SubmissionDate": ("le", "ge", "lt", "gt", "eq"),
        "CompletedDate": ("le", "ge", "lt", "gt", "eq"),
        "WorkflowId": ("eq",),
        "Status": ("eq",),
        "InputProductReference": ("eq",),
    }
    for key, op, value in filters:
        if key not in set(allowed_filters):
            raise ValueError(
                f"{key!r} is not an allowed key, Transformation Orders can "
                f"be filtered using only the following keys: {list(allowed_filters)}"
            )
        if op not in allowed_filters[key]:
            raise ValueError(
                f"{op!r} is not an allowed operator for key {key!r}; "
                f"the valid operators are: {allowed_filters[key]!r}"
            )


def get_transformation_orders(
    filters: T.List[T.Tuple[str, str, str]] = [], uri_root: str = None,
) -> T.List[T.Dict["str", T.Any]]:
    """
    Return the all the transformation orders.
    They can be filtered by the SubmissionDate, CompletedDate, Status
    :param T.List[T.Tuple[str, str, str]] filters: list of tuple
    """
    # check filters
    check_filter_validity(filters)
    transformation_orders = []
    for order_id in TRANSFORMATION_ORDERS.keys():
        transformation_order = get_transformation_order(order_id, uri_root=uri_root)
        add_order = True
        for key, op, value in filters:
            if key == "CompletedDate" and "CompletedDate" not in transformation_order:
                add_order = False
                continue
            op = getattr(operator, op)
            if key == "InputProductReference":
                order_value = transformation_order["InputProductReference"]["Reference"]
            else:
                order_value = transformation_order[key]
            if key in {"CompletedDate", "SubmissionDate"}:
                order_value = datetime.fromisoformat(order_value)
                try:
                    value = datetime.fromisoformat(value)
                except ValueError:
                    raise ValueError(
                        f"{key!r} is not a valid isoformat string: {value}"
                    )
            add_order = add_order and op(order_value, value)
        if add_order:
            transformation_orders.append(transformation_order)
    return transformation_orders


def extract_workflow_defaults(config_workflow_options):
    """
    Extract default values from plugin workflow declaration
    """
    default_options = {}
    for option_name, option in config_workflow_options.items():
        if "Default" in option:
            default_options[option_name] = option["Default"]
    return default_options


def extract_config_options_names(config_workflow_options):
    """extract options names from config_workflow_options"""
    options_names = []
    for option in config_workflow_options:
        options_names.append(option["Name"])
    return options_names


def fill_with_defaults(workflow_options, config_workflow_options, workflow_id=None):
    """
    Fill the missing workflow options with the defaults values declared in the plugin
    """
    default_options = extract_workflow_defaults(config_workflow_options)
    workflow_options = {**default_options, **workflow_options}

    missing_options_values = set(config_workflow_options) - set(workflow_options)
    if len(missing_options_values):
        raise ValueError(
            f"the following missing options are mandatory for {workflow_id!r} Workflow:"
            f"{list(missing_options_values)!r} "
        )
    return workflow_options


@functools.lru_cache()
def read_users_quota(users_quota_file):
    """Return the users' quota as read from the dedicated configuration file. The keys are the
    possible roles and the values are the roles' cap.

    :param str users_quota_file: full path to the users' quota configuration file
    :return dict:
    """
    with open(users_quota_file) as file:
        users_quota = yaml.load(file, Loader=yaml.FullLoader)
    return users_quota


def check_user_quota(user_id, user_roles, users_quota_file=None):
    """Check the user's quota to determine if he is able to submit a transformation or not. If the
    cap has been already reached, a RuntimeError is raised.

    :param str user_id:
    :param str user_roles:
    :param str users_quota_file:
    :return:
    """
    if (user_id not in USERS_TRANSFORMATIONS) or (user_roles is None):
        return
    if users_quota_file is None:
        users_quota_file = os.getenv("USERS_QUOTA_FILE", "./users_quota.yaml")
    if not os.path.isfile(users_quota_file):
        raise ValueError(
            f"{users_quota_file} not found, please define it using 'users_quota_file' "
            "keyword argument or the environment variable USERS_QUOTA_FILE"
        )
    file_modification_time = datetime.fromtimestamp(os.path.getmtime(users_quota_file))
    if (datetime.now() - file_modification_time).total_seconds() < 3600:
        read_users_quota.cache_clear()
    users_quota = read_users_quota(users_quota_file)
    running_processes = 0
    for order_id in USERS_TRANSFORMATIONS[user_id]:
        order_status = get_transformation_order(order_id)["Status"]
        running_processes += order_status == "in_progress"
    user_caps = [users_quota.get(role) for role in user_roles if users_quota.get(role)]
    user_cap = max(user_caps) if user_caps else running_processes + 1
    if running_processes >= user_cap:
        raise RuntimeError(
            f"the user '{user_id}' has reached his user quota: "
            f"{running_processes} processes are running"
        )


def submit_workflow(
    workflow_id,
    *,
    input_product_reference,
    workflow_options,
    user_id=None,
    user_roles=None,
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
    {'Reference': 'S2A_MSIL1C_20170205T105221_N0204_R051_T31TCF_20170205T105426', 'api_hub': 'scihub'}
    :param dict workflow_options: dictionary containing the workflow kwargs
    :param str user_id: user identifier
    :param str user_roles: list of the user roles
    :param str working_dir: optional working directory within which will be created the processing directory.
    If it is None it is used the value of the environment variable "WORKING_DIR"
    :param str output_dir: optional output directory. If it is None it is used the value of the environment
    variable "OUTPUT_DIR"
    :param str hubs_credentials_file: optional file of credentials. If it is None is used the value
    of the environment variable "HUBS_CREDENTIALS_FILE"
    :param str scheduler:  optional the scheduler to be used fot the client instantiation. If it is None it will be used
    the value of environment variable SCHEDULER
    :param order_id:
    """
    workflow = get_workflow_by_id(workflow_id)
    check_user_quota(user_id, user_roles)
    product_type = workflow["InputProductType"]
    check_products_consistency(
        product_type, input_product_reference["Reference"], workflow_id=workflow_id
    )
    if not order_id:
        order_id = dask.base.tokenize(
            workflow_id, input_product_reference, workflow_options,
        )

    workflow_options = fill_with_defaults(
        workflow_options, workflow["WorkflowOptions"], workflow_id=workflow_id,
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

    client = instantiate_client(scheduler)

    if user_id:
        logger.info(
            f"submitting transformation order {order_id!r} request by user {user_id!r}"
        )
    else:
        logger.info(f"submitting transformation order {order_id!r}")

    if order_id in TRANSFORMATION_ORDERS:
        order = TRANSFORMATION_ORDERS[order_id]
        future = order["future"]
        if future.status == "error":
            client.retry(future)
            order.pop("CompletedDate", None)
            future.add_done_callback(add_completed_date)
            order["SubmissionDate"] = datetime.now().isoformat()
    else:
        future = client.submit(task, key=order_id)
        order = {
            "future": future,
            "Id": order_id,
            "SubmissionDate": datetime.now().isoformat(),
            "InputProductReference": input_product_reference,
            "WorkflowOptions": workflow_options,
            "WorkflowId": workflow_id,
            "WorkflowName": workflow["WorkflowName"],
        }
        TRANSFORMATION_ORDERS[order_id] = order
    if user_id in USERS_TRANSFORMATIONS:
        USERS_TRANSFORMATIONS[user_id].append(order_id)
    else:
        USERS_TRANSFORMATIONS[user_id] = [order_id]
        future.add_done_callback(add_completed_date)

    return build_transformation_order(order)
