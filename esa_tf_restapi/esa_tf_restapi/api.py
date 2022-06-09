import functools
import logging
import os
import re
import typing as T
from datetime import datetime

import dask.distributed
import pydantic
import yaml

from .auth import DEFAULT_USER
from .transformation_orders import Queue, TransformationOrder

logger = logging.getLogger(__name__)

queue = Queue()
CLIENT = None
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


class ConfigurationError(Exception):
    pass


class RequestError(Exception):
    def __init__(self, user_id, message):
        self.user_id = user_id
        self.message = message
        super().__init__(self.message)


class ExceededQuota(Exception):
    def __init__(self, user_id, message):
        self.user_id = user_id
        self.message = message
        super().__init__(self.message)


class ItemNotFound(Exception):
    def __init__(self, user_id, message):
        self.user_id = user_id
        self.message = message
        super().__init__(self.message)


class Configuration(pydantic.BaseModel):

    keeping_period: int = 14400
    excluded_workflows: T.List[str] = []
    enable_traceability: bool = True
    enable_authorization_check: bool = True
    enable_quota_check: bool = True
    default_role: T.TypedDict("Role", quota=int, profile=str) = {
        "quota": 1,
        "profile": "user",
    }
    roles: T.Dict[str, T.TypedDict("Role", quota=int, profile=str)] = {}


def check_products_consistency(
    product_type, input_product_reference_name, workflow_id=None, user_id=DEFAULT_USER
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
        raise RequestError(
            user_id,
            f"input product name {input_product_reference_name!r} does not comply "
            f"to the naming convention for the {product_type!r} product type required by "
            f"{workflow_id!r}",
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
    workflow_cleaned = {}
    for name in workflows:
        workflow_cleaned[name] = {
            "Id": workflows[name]["Id"],
            "WorkflowName": workflows[name]["WorkflowName"],
            "Description": workflows[name]["Description"],
            "InputProductType": workflows[name]["InputProductType"],
            "OutputProductType": workflows[name]["OutputProductType"],
            "WorkflowVersion": workflows[name]["WorkflowVersion"],
            "WorkflowOptions": workflows[name]["WorkflowOptions"],
        }
    return workflow_cleaned


def get_workflow_by_id(workflow_id, esa_tf_config=None, user_id=DEFAULT_USER):
    """
    Return the workflow configuration corresponding to the workflow_id.
    """
    # definition of the task must be internal
    # to avoid dask to import esa_tf_restapi in the workers
    workflows = get_workflows(esa_tf_config=esa_tf_config)
    try:
        workflow = workflows[workflow_id]
    except KeyError:
        raise ItemNotFound(
            user_id,
            f"Workflow {workflow_id!r} not found, the available workflows are {list(workflows)!r}",
        )
    return workflow


def get_workflows(product_type=None, esa_tf_config=None):
    """
    Return the workflows configurations installed in the workers.
    They may be filtered using the product type
    """
    if esa_tf_config is None:
        esa_tf_config = read_esa_tf_config()
    excluded_workflows = esa_tf_config["excluded_workflows"]
    workflows = {}
    for workflow_id, workflow in get_all_workflows().items():
        if workflow_id not in excluded_workflows:
            workflows[workflow_id] = workflow

    if product_type:
        filtered_workflows = {}
        for name in workflows:
            if product_type == workflows[name]["InputProductType"]:
                filtered_workflows[name] = workflows[name]
        return filtered_workflows
    return workflows


def get_transformation_order_log(
    order_id, user_id=DEFAULT_USER, filter_by_user_id=True
):
    transformation_orders = queue.get_transformation_orders(
        user_id=user_id, filter_by_user_id=filter_by_user_id
    )
    if order_id not in transformation_orders:
        raise ItemNotFound(user_id, f"Transformation Order {order_id!r} not found")
    return transformation_orders[order_id].get_log()


def get_transformation_order(order_id, user_id=DEFAULT_USER, filter_by_user_id=True):
    """
    Return the transformation order corresponding to the order_id
    """
    transformation_orders = queue.get_transformation_orders(
        user_id=user_id, filter_by_user_id=filter_by_user_id
    )
    if order_id not in transformation_orders:
        raise ItemNotFound(user_id, f"Transformation Order {order_id!r} not found")
    return transformation_orders[order_id].get_info()


def check_filter_validity(filters, user_id=DEFAULT_USER):
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
            raise RequestError(
                user_id,
                f"{key!r} is not an allowed key, Transformation Orders can "
                f"be filtered using only the following keys: {list(allowed_filters)}",
            )
        if op not in allowed_filters[key]:
            raise RequestError(
                user_id,
                f"{op!r} is not an allowed operator for key {key!r}; "
                f"the valid operators are: {allowed_filters[key]!r}",
            )
        if key in {"CompletedDate", "SubmissionDate"}:
            try:
                datetime.fromisoformat(value)
            except ValueError:
                raise RequestError(
                    user_id, f"{key!r} is not a valid isoformat string: {value}"
                )


def extract_roles_key(
    esa_tf_config, user_roles=[], key="profile", user_id=DEFAULT_USER
):
    """Return the profiles associated with the user's roles input list.
    :param dict esa_tf_config: esa_tf configuration dictionary
    :param list user_roles: user roles
    :param str key: it can be "profile" or "quota"
    :param str user_id: user ID
    :return set:
    """
    roles_config = esa_tf_config.get("roles", {})
    default_role = esa_tf_config["default_role"]

    if not user_roles:
        logger.warning(
            f"user: {user_id!r} - role not defined, a default {key!r} will be used: {default_role[key]!r}"
        )
        return [default_role[key]]

    values = []

    for user_role in user_roles:
        value = roles_config.get(user_role, {}).get(key)
        if value is None:
            logger.warning(
                f"user: {user_id} - {key} not defined for user role {user_role}"
            )
        else:
            values.append(value)

    if not values:
        logger.warning(
            f"user: {user_id} - {key!r} not defined for user roles {user_roles!r}, a default will be used: {default_role[key]!r}"
        )
        values.append(default_role[key])

    return values


def get_profile(user_roles: list = [], user_id=DEFAULT_USER):
    esa_tf_config = read_esa_tf_config()
    if not esa_tf_config["enable_authorization_check"]:
        return "manager"

    user_profiles = extract_roles_key(
        esa_tf_config, user_roles, key="profile", user_id=user_id
    )
    if "manager" in user_profiles:
        return "manager"
    elif "user" in user_profiles:
        return "user"
    else:
        return "unauthorized"


def get_transformation_orders(
    filters: T.List[T.Tuple[str, str, str]] = [],
    user_id: str = DEFAULT_USER,
    filter_by_user_id: str = True,
) -> T.List[T.Dict["str", T.Any]]:
    """
    Return the all the transformation orders.
    They can be filtered by the SubmissionDate, CompletedDate, Status
    :param T.List[T.Tuple[str, str, str]] filters: list of tuple defining the filter to be applied
    :param str user_id: user ID
    :param bool filter_by_user_id: if True the transformation orders are filtered by the user_id
    """
    # check filters
    check_filter_validity(filters, user_id=user_id)
    transformation_orders = queue.get_transformation_orders(
        filters=filters, user_id=user_id, filter_by_user_id=filter_by_user_id
    )
    return [order.get_info() for order in transformation_orders.values()]


def extract_workflow_defaults(config_workflow_options):
    """
    Extract default values from plugin workflow declaration
    """
    default_options = {}
    for option_name, option in config_workflow_options.items():
        if "Default" in option:
            default_options[option_name] = option["Default"]
    return default_options


def fill_with_defaults(
    workflow_options, config_workflow_options, workflow_id=None, user_id=DEFAULT_USER
):
    """
    Fill the missing workflow options with the defaults values declared in the plugin
    """
    default_options = extract_workflow_defaults(config_workflow_options)
    workflow_options = {**default_options, **workflow_options}

    missing_options_values = set(config_workflow_options) - set(workflow_options)
    if len(missing_options_values):
        raise RequestError(
            user_id,
            f"the following missing options are mandatory for {workflow_id!r} Workflow:"
            f"{list(missing_options_values)!r} ",
        )
    return workflow_options


def check_user_quota(user_id, user_roles=None, esa_tf_config=None):
    """Check the user's quota to determine if he is able to submit a transformation or not. If the
    cap has been already reached, a ExceededQuota is raised.

    :param dict esa_tf_config: esa_tf configuration dictionary containing the quotas and the key enable_quota_check
    :param str user_id: user identifier
    :param str user_roles: list of the user roles
    :return:
    """
    if esa_tf_config is None:
        esa_tf_config = read_esa_tf_config()
    if not esa_tf_config["enable_quota_check"]:
        return

    user_quotas = extract_roles_key(
        esa_tf_config, user_roles, key="quota", user_id=user_id
    )
    user_cap = max(user_quotas)
    running_processes = queue.get_count_uncompleted_orders(user_id)
    if running_processes >= user_cap:
        raise ExceededQuota(
            user_id,
            f"the user {user_id!r} has reached his quota: {running_processes!r} processes are running",
        )


def read_esa_tf_config():
    """
    :return dict: returns esa_tf_config dictionary
    """
    esa_tf_config_file = os.getenv("ESA_TF_CONFIG_FILE", "./esa_tf.config")
    if not os.path.isfile(esa_tf_config_file):
        raise FileNotFoundError(
            f"{esa_tf_config_file!r} not found, please define the correct path "
            f"using the environment variable ESA_TF_CONFIG_FILE"
        )
    with open(esa_tf_config_file) as file:
        esa_tf_config = yaml.load(file, Loader=yaml.FullLoader)
    try:
        esa_tf_configuration_object = Configuration(**esa_tf_config)
    except ValueError as exc:
        raise ConfigurationError(
            f"invalid configuration file esa_tf.config: {exc!r}"
        ) from exc
    return esa_tf_configuration_object.dict()


def evict_orders(esa_tf_config=None):
    """Evict orders from the queue according to a
    configurable keeping period parameter. The keeping period parameter is based on the CompletedDate
    (i.e. the datetime when the output product of a TransformationOrders is available in the
    staging area). The keeping period parameter is expressed in minutes and is defined in the
    esa_tf.config file.

    :param dict esa_tf_config: esa_tf configuration dictionary containing key keeping_period

    :return:
    """
    # if the "esa_tf_config_file" configuration file has been changed in the amount of time
    # specified by the "FILE_MODIFICATION_INTERVAL" constant value, then the cache is cleared
    if esa_tf_config is None:
        esa_tf_config = read_esa_tf_config()
    keeping_period = esa_tf_config["keeping_period"]
    queue.remove_old_orders(keeping_period)


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
    If it is None it is used the value of the environment variable "WORKING_DIR"
    variable "OUTPUT_DIR"
    of the environment variable "HUBS_CREDENTIALS_FILE"
    """
    # a default role is used if user_roles is equal to None or [], [None], [None, None, ...]
    esa_tf_config = read_esa_tf_config()
    evict_orders(esa_tf_config=esa_tf_config)
    check_user_quota(
        user_id=user_id, user_roles=user_roles, esa_tf_config=esa_tf_config
    )
    workflow = get_workflow_by_id(workflow_id, esa_tf_config=esa_tf_config)

    check_products_consistency(
        workflow["InputProductType"],
        input_product_reference["Reference"],
        workflow_id=workflow_id,
        user_id=user_id,
    )
    workflow_options = fill_with_defaults(
        workflow_options,
        workflow["WorkflowOptions"],
        workflow_id=workflow_id,
        user_id=user_id,
    )
    order_id = dask.base.tokenize(
        workflow_id,
        input_product_reference,
        workflow_options,
        esa_tf_config["enable_traceability"],
    )
    logger.info(f"user: {user_id!r} - submitting transformation order {order_id!r}")
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
            enable_traceability=esa_tf_config["enable_traceability"],
            uri_root=uri_root,
        )

    queue.add_order(transformation_order, user_id=user_id)

    return transformation_order.get_info()
