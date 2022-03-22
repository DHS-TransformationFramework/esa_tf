import asyncio
import copy
import functools
import logging
import operator
import os
import re
import typing as T
from datetime import datetime

import dask.distributed
import yaml

from .auth import DEFAULT_USER

logger = logging.getLogger(__name__)

CLIENT = None
TRANSFORMATION_ORDERS = {}
USERS_TRANSFORMATION_ORDERS = {}
DEFAULT_ESA_TF_ROLE = "default_esa_tf_role"
FILE_MODIFICATION_INTERVAL = 86400  # sec


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


def _prepare_download_uri(
    output_product_referece: list, root_uri: T.Optional[str] = None
):
    for product in output_product_referece:
        if root_uri:
            download_uri = f"{root_uri}download/{product['ReferenceBasePath']}/{product['Reference']}"
        else:
            download_uri = None
        product["DownloadURI"] = download_uri
        del product["ReferenceBasePath"]
    return output_product_referece


def add_completed_info(future):
    order = TRANSFORMATION_ORDERS[future.key]
    order["CompletedDate"] = datetime.now().isoformat()
    basepath, reference = os.path.split(future.result())
    order["OutputProductReference"] = [
        {"Reference": reference, "ReferenceBasePath": basepath}
    ]


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


def get_workflow_by_id(workflow_id):
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


def get_workflows(product=None):
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


def resubmit_transformation_order(future):
    client = instantiate_client()
    order = TRANSFORMATION_ORDERS[future.key]
    client.retry(future)
    order.pop("CompletedDate", None)
    order.pop("OutputProductReference", None)
    order["SubmissionDate"] = datetime.now().isoformat()
    future.add_done_callback(add_completed_info)
    return order


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


def read_roles_config(roles_config_file):
    """Return the users' quota as read from the dedicated configuration file. The keys are the
    possible roles and the values are the roles' cap.

    :param str roles_config_file: full path to the roles' configuration file
    :return dict:
    """
    with open(roles_config_file) as file:
        users_quota = yaml.load(file, Loader=yaml.FullLoader)
    if DEFAULT_ESA_TF_ROLE not in users_quota:
        raise RuntimeError(
            f"default role 'default_tf_role' not found in {roles_config_file}. "
            f"Please, add the default role into the configuration file {roles_config_file}"
        )
    return users_quota


def get_roles_config_filepath(roles_config_file=None):
    """Return the path of `roles.config` file and perform a check about the existence of the
    path.

    :param str roles_config_file: full path to the roles' configuration file
    :return str:
    """
    if roles_config_file is None:
        roles_config_file = os.getenv("ROLES_CONFIG_FILE", "./roles.config")
    if not os.path.isfile(roles_config_file):
        raise ValueError(
            f"{roles_config_file} not found, please define it using 'role_config_file' "
            "keyword argument or the environment variable ROLES_CONFIG_FILE"
        )
    return roles_config_file


def get_profiles(user_roles, roles_config_file=None):
    """Return the profiles associated with the user's roles input list.

    :param list user_roles: user roles
    :param str roles_config_file: full path to the roles' configuration file
    :return set:
    """
    roles_config_file = get_roles_config_filepath(roles_config_file)
    roles = read_roles_config(roles_config_file)
    profiles = set()
    for role in user_roles:
        profiles.add(roles.get(role, roles[DEFAULT_ESA_TF_ROLE]).get("profile"))
    return profiles


def has_manager_profile(user_roles: list = []):
    return "manager" in get_profiles(user_roles)


def get_transformation_orders(
    filters: T.List[T.Tuple[str, str, str]] = [],
    user_id: str = DEFAULT_USER,
    uri_root: str = None,
) -> T.List[T.Dict["str", T.Any]]:
    """
    Return the all the transformation orders.
    They can be filtered by the SubmissionDate, CompletedDate, Status
    :param T.List[T.Tuple[str, str, str]] filters: list of tuple
    :param str user_id: user identifier
    :param str user_roles: user role
    """
    # check filters
    check_filter_validity(filters)
    transformation_orders = []
    if user_id == DEFAULT_USER:
        orders_ids = list(TRANSFORMATION_ORDERS)
    else:
        orders_ids = USERS_TRANSFORMATION_ORDERS.get(user_id, [])

    for order_id in orders_ids:
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


def reckon_running_process(user_id):
    """Return the number of running processes (i.e. status equal to `in_progress`) among those
    required by the `user_id`.

    :param str user_id: user identifier
    :return int:
    """
    running_processes = 0
    for order_id in USERS_TRANSFORMATION_ORDERS[user_id]:
        order_status = get_transformation_order(order_id)["Status"]
        running_processes += order_status == "in_progress"
    return running_processes


def reckon_user_quota_cap(user_roles, roles_config_file, user_id=DEFAULT_USER):
    """Return the cap of "in_progress" processes according to the role(s). If more than a role has
    been specified, the cap is calculated as the largest cap among those corresponding to the roles
    list.

    :param list_or_tuple user_roles: list of the roles associated with the user
    :param str roles_config_file: path of the configuration file with the users' quota by roles
    :param str user_id: user identifier
    :return int:
    """
    users_quota = read_roles_config(roles_config_file)
    user_caps = []
    # please, note that the user_roles list is never empty because if no role has been defined the
    # GENERAL_TF_ROLE is set in submit_workflow function
    for role in user_roles:
        cap = users_quota.get(role, {}).get("quota")
        if cap is None:
            logger.info(
                f"role '{role}' not found in the configuration file {roles_config_file}",
                extra=dict(user=user_id),
            )
        else:
            user_caps.append(cap)
    # if all roles are not present on the configuration file, then the GENERAL_TF_ROLE is used
    if not user_caps:
        logger.warning(
            f"no role among those defined for the user was found in the configuration file {roles_config_file}, "
            f"a general TF role will be used",
            extra=dict(user=user_id),
        )
    user_cap = max(user_caps, default=users_quota.get(DEFAULT_ESA_TF_ROLE).get("quota"))
    return user_cap


def check_user_quota(user_id, user_roles, roles_config_file=None):
    """Check the user's quota to determine if he is able to submit a transformation or not. If the
    cap has been already reached, a RuntimeError is raised.

    :param str user_id: user identifier
    :param str user_roles: list of the user roles
    :param str roles_config_file: full path to the `roles.config` file
    :return:
    """
    if user_roles is None or not any(user_roles):
        logger.warning(
            f"no user-role is defined, a default TF role will be used",
            extra=dict(user=user_id),
        )
        user_roles = [DEFAULT_ESA_TF_ROLE]
    roles_config_file = get_roles_config_filepath(roles_config_file)

    user_cap = reckon_user_quota_cap(user_roles, roles_config_file, user_id)
    if user_id not in USERS_TRANSFORMATION_ORDERS:
        return
    running_processes = reckon_running_process(user_id)
    if running_processes >= user_cap:
        raise RuntimeError(
            f"the user {user_id} has reached his quota: {running_processes} processes are running"
        )


def read_esa_tf_config(esa_tf_config_file):
    """

    :param str esa_tf_config_file: full path to the `esa_tf.config` file
    :return dict:
    """
    with open(esa_tf_config_file) as file:
        esa_tf_config = yaml.load(file, Loader=yaml.FullLoader)
    return esa_tf_config


def update_orders_dicts(keeping_period):
    """Update the TRANSFORMATION_ORDERS and USERS_TRANSFORMATIONS dictionaries removing only the
    transformations with statuses `completed` or `failed` that are older than the `keeping_period`.
    It returns the list of order-IDs that have been deleted.

    :param int keeping_period: the minimum number of minutes from the CompletedDate that a
    TransformationOrder will be kept in memory
    :return list:
    """
    tformat = "%Y-%m-%dT%H:%M:%S"
    now = datetime.now()
    # find completed or failed orders that are older than keeping_period
    orders_to_delete = []
    for order_id, order in TRANSFORMATION_ORDERS.items():
        status = get_transformation_order(order_id)["Status"]
        if status != "in_progress":
            completed_date_str = order.get("CompletedDate", now.isoformat())
            completed_date = datetime.strptime(
                completed_date_str.split(".")[0], tformat
            )
            elapsed_minutes = (
                now - completed_date
            ).total_seconds() / 60  # from sec to minutes
            if elapsed_minutes > keeping_period:
                orders_to_delete.append(order_id)

    # remove old orders from the TRANSFORMATION_ORDERS dictionary
    for order_id in orders_to_delete:
        TRANSFORMATION_ORDERS.pop(order_id, None)

    # remove old orders from the USERS_TRANSFORMATIONS dictionary
    for user_id, orders_ids in USERS_TRANSFORMATION_ORDERS.items():
        orders_to_keep = orders_ids.difference(orders_to_delete)
        USERS_TRANSFORMATION_ORDERS[user_id] = orders_to_keep
    return orders_to_delete


async def evict_orders(esa_tf_config_file=None):
    """Evict orders from the TRANSFORMATION_ORDERS and USERS_TRANSFORMATIONS according to a
    configurable keeping period parameter. The keeping period parameter is based on the CompletedDate
    (i.e. the datetime when the output product of a TransformationOrders is available in the
    staging area). The keeping period parameter is expressed in minutes and is defined in the
    esa_tf.config file.

    :param str esa_tf_config_file: full path to the `esa_tf.config` file
    :return:
    """
    # if the "esa_tf_config_file" configuration file has been changed in the amount of time
    # specified by the "FILE_MODIFICATION_INTERVAL" constant value, then the cache is cleared
    if esa_tf_config_file is None:
        esa_tf_config_file = os.getenv("ESA_TF_CONFIG_FILE", "./esa_tf.config")
    if not os.path.isfile(esa_tf_config_file):
        raise ValueError(
            f"{esa_tf_config_file} not found, please define it using 'esa_tf_config_file' "
            "keyword argument or the environment variable ESA_TF_CONFIG_FILE"
        )

    esa_tf_config = read_esa_tf_config(esa_tf_config_file)
    keeping_period = esa_tf_config.get("keeping-period")
    update_orders_dicts(keeping_period)
    return keeping_period


def submit_workflow(
    workflow_id,
    *,
    input_product_reference,
    workflow_options,
    user_id=DEFAULT_USER,
    user_roles=None,
    working_dir=None,
    output_dir=None,
    hubs_credentials_file=None,
    order_id=None,
    uri_root=None,
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
    :param order_id:
    """
    # a default role is used if user_roles is equal to None or [], [None], [None, None, ...]
    check_user_quota(user_id, user_roles)
    asyncio.create_task(evict_orders())
    workflow = get_workflow_by_id(workflow_id)
    product_type = workflow["InputProductType"]
    check_products_consistency(
        product_type, input_product_reference["Reference"], workflow_id=workflow_id
    )
    if not order_id:
        order_id = dask.base.tokenize(
            workflow_id, input_product_reference, workflow_options,
        )
    logger.info(
        f"submitting transformation order {order_id!r}", extra=dict(user=user_id)
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

    client = instantiate_client()

    if order_id in TRANSFORMATION_ORDERS:
        order = TRANSFORMATION_ORDERS[order_id]
        future = order["future"]
        if future.status == "error":
            client.retry(future)
            order.pop("CompletedDate", None)
            order.pop("OutputProductReference", None)
            order["SubmissionDate"] = datetime.now().isoformat()
            future.add_done_callback(add_completed_info)
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
        future.add_done_callback(add_completed_info)

    if user_id in USERS_TRANSFORMATION_ORDERS:
        USERS_TRANSFORMATION_ORDERS[user_id].add(order_id)
    else:
        USERS_TRANSFORMATION_ORDERS[user_id] = set((order_id,))

    return build_transformation_order(order, uri_root=uri_root)
