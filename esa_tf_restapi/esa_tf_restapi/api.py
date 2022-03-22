import asyncio
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
from .transformation_orders import Queue, TransformationOrder
logger = logging.getLogger(__name__)

queue = Queue()
CLIENT = None
DEFAULT_ESA_TF_ROLE = "default_esa_tf_role"
FILE_MODIFICATION_INTERVAL = 86400  # sec

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


def get_workflows(product_type=None):
    """
    Return the workflows configurations installed in the workers.
    They may be filtered using the product type
    """
    workflows = get_all_workflows()
    if product_type:
        filtered_workflows = {}
        for name in workflows:
            if product_type == workflows[name]["InputProductType"]:
                filtered_workflows[name] = workflows[name]
        return filtered_workflows
    return workflows


def get_transformation_order_log(order_id):
    if order_id not in queue.transformation_orders:
        raise KeyError(f"Transformation Order {order_id!r} not found")
    return queue.transformation_orders[order_id].get_log()


def get_transformation_order(order_id):
    """
    Return the transformation order corresponding to the order_id
    """
    if order_id not in queue.transformation_orders:
        raise KeyError(f"Transformation Order {order_id!r} not found")
    return queue.transformation_orders[order_id].get_info()


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
    filters: T.List[T.Tuple[str, str, str]] = []
) -> T.List[T.Dict["str", T.Any]]:
    """
    Return the all the transformation orders.
    They can be filtered by the SubmissionDate, CompletedDate, Status
    :param T.List[T.Tuple[str, str, str]] filters: list of tuple
    """
    # check filters
    check_filter_validity(filters)
    valid_transformation_orders_orders = []
    for order_id in queue.transformation_orders:
        transformation_order = queue.transformation_orders[order_id].get_info()
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
            valid_transformation_orders_orders.append(transformation_order)
    return valid_transformation_orders_orders


def extract_workflow_defaults(config_workflow_options):
    """
    Extract default values from plugin workflow declaration
    """
    default_options = {}
    for option_name, option in config_workflow_options.items():
        if "Default" in option:
            default_options[option_name] = option["Default"]
    return default_options


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


def read_users_quota():
    """Return the users' quota as read from the dedicated configuration file. The keys are the
    possible roles and the values are the roles' cap.

    :param str users_quota_file: full path to the users' quota configuration file
    :return dict:
    """
    users_quota_file = os.getenv("USERS_QUOTA_FILE", "./users_quota.yaml")
    if not os.path.isfile(users_quota_file):
        raise ValueError(
            f"{users_quota_file} not found, please define it using 'users_quota_file' "
            "keyword argument or the environment variable USERS_QUOTA_FILE"
        )
    with open(users_quota_file) as file:
        users_quota = yaml.load(file, Loader=yaml.FullLoader)
    if DEFAULT_ESA_TF_ROLE not in users_quota:
        raise RuntimeError(
            f"default role 'default_tf_role' not found in {users_quota_file}. "
            f"Please, add the default role into the configuration file {users_quota_file}"
        )
    return users_quota


def get_user_quota_cap(user_roles, user_id=DEFAULT_USER):
    """Return the cap of "in_progress" processes according to the role(s). If more than a role has
    been specified, the cap is calculated as the largest cap among those corresponding to the roles
    list.

    :param list_or_tuple user_roles: list of the roles associated with the user
    :param str user_id: user identifier
    :return int:
    """
    users_quota = read_users_quota()
    user_caps = []
    # please, note that the user_roles list is never empty because if no role has been defined the
    # GENERAL_TF_ROLE is set in submit_workflow function
    for role in user_roles:
        cap = users_quota.get(role, {}).get("submit_limit")
        if cap is None:
            logger.info(
                f"user: {user_id} - role '{role}' not found in the configuration file",
            )
        else:
            user_caps.append(cap)
    # if all roles are not present on the configuration file, then the GENERAL_TF_ROLE is used
    if not user_caps:
        logger.warning(
            f"user: {user_id} - no role among those defined for the user was found in the configuration file, "
            f"a general TF role will be used",
        )
    user_cap = max(
        user_caps, default=users_quota.get(DEFAULT_ESA_TF_ROLE).get("submit_limit")
    )
    return user_cap


def check_user_quota(user_id, user_roles):
    """Check the user's quota to determine if he is able to submit a transformation or not. If the
    cap has been already reached, a RuntimeError is raised.

    :param str user_id: user identifier
    :param str user_roles: list of the user roles
    :return:
    """
    if user_roles is None or not any(user_roles):
        logger.warning(
            f"user: {user_id} - no user-role is defined, a default TF role will be used",
        )
        user_roles = [DEFAULT_ESA_TF_ROLE]

    user_cap = get_user_quota_cap(user_roles, user_id)
    if user_id not in queue.user_to_orders:
        return
    running_processes = queue.get_count_uncompleted_orders(user_id)
    if running_processes >= user_cap:
        raise RuntimeError(
            f"the user {user_id} has reached his quota: {running_processes} processes are running"
        )


def read_esa_tf_config():
    """
    :param str esa_tf_config_file: full path to the `esa_tf.config` file
    :return dict:
    """
    esa_tf_config_file = os.getenv("ESA_TF_CONFIG_FILE", "./esa_tf.config")
    if not os.path.isfile(esa_tf_config_file):
        raise ValueError(
            f"{esa_tf_config_file} not found, please define it using 'esa_tf_config_file' "
            "keyword argument or the environment variable ESA_TF_CONFIG_FILE"
        )
    with open(esa_tf_config_file) as file:
        esa_tf_config = yaml.load(file, Loader=yaml.FullLoader)
    return esa_tf_config


async def evict_orders():
    """Evict orders from the queue according to a
    configurable keeping period parameter. The keeping period parameter is based on the CompletedDate
    (i.e. the datetime when the output product of a TransformationOrders is available in the
    staging area). The keeping period parameter is expressed in minutes and is defined in the
    esa_tf.config file.

    :param str esa_tf_config_file: full path to the `esa_tf.config` file
    :return:
    """
    # if the "esa_tf_config_file" configuration file has been changed in the amount of time
    # specified by the "FILE_MODIFICATION_INTERVAL" constant value, then the cache is cleared
    esa_config = read_esa_tf_config()
    keeping_period = esa_config.get("keeping-period")
    queue.remove_old_orders(keeping_period)
    return keeping_period


def submit_workflow(
    workflow_id,
    *,
    input_product_reference,
    workflow_options,
    user_id=DEFAULT_USER,
    user_roles=None,
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
    asyncio.create_task(evict_orders())
    check_user_quota(user_id, user_roles)
    workflow = get_workflow_by_id(workflow_id)
    check_products_consistency(
        workflow["InputProductType"],
        input_product_reference["Reference"],
        workflow_id=workflow_id
    )
    workflow_options = fill_with_defaults(
        workflow_options, workflow["WorkflowOptions"], workflow_id=workflow_id,
    )
    order_id = dask.base.tokenize(
            workflow_id, input_product_reference, workflow_options,
        )
    logger.info(
        f"user: {user_id} - submitting transformation order {order_id!r}"
    )
    if order_id in queue.transformation_orders:
        transformation_order = queue.transformation_orders[order_id]
        transformation_order.resubmit()
    else:
        client = instantiate_client()
        transformation_order = TransformationOrder.submit(
            client=client,
            order_id=order_id,
            product_reference=input_product_reference,
            workflow_id=workflow_id,
            workflow_options=workflow_options,
            workflow_name=workflow["WorkflowName"]
        )
        transformation_order.uri_root = uri_root
        queue.add_order(transformation_order, user_id=user_id)

    return transformation_order.get_info()


