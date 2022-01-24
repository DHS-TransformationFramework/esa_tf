import copy
import functools
import os
import re
from datetime import datetime

import dask.distributed

CLIENT = None
TRANSFORMATION_ORDERS = {}


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
            f"Workflow {workflow_id} product type not recognized. product type shall"
            f"one of the following {SENTINEL1}, {SENTINEL2}"
        )

    if not re.match(exp, str(input_product_reference_name)):
        raise ValueError(
            f"The input product reference name {input_product_reference_name} "
            f"is not compliant with product type {product_type} in workflow {workflow_id}"
        )


def instantiate_client(scheduler_addr=None):
    """
    Return a client with a scheduler with address ``scheduler_addr``.
    """
    global CLIENT

    if scheduler_addr is None:
        scheduler_addr = os.getenv("SCHEDULER")
    if scheduler_addr is None:
        raise ValueError("scheduler not defined")

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
            f"Workflow {workflow_id} not found, available workflows are {list(workflows.keys())}"
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


def build_transformation_order(order):
    # Note: the future must be extracted from the original order. The deepcopy breaks the future
    future = order["future"]
    transformation_order = copy.deepcopy(order)
    transformation_order.pop("future")
    transformation_order["Status"] = STATUS_DASK_TO_API[future.status]

    if future.status == "finished":
        transformation_order["OutputFile"] = os.path.basename(future.result())
    return transformation_order


def get_transformation_order(order_id):
    """
    Return the transformation order corresponding to the order_id
    """
    order = TRANSFORMATION_ORDERS.get(order_id)
    if order is None:
        raise KeyError(f"Transformation Order {order_id} not found")
    transformation_order = build_transformation_order(order)
    return transformation_order


def get_transformation_orders(status=None, workflow_id=None):
    """
    Return the all the transformation orders.
    The can be filtered by the status and the workflow_id
    """
    transformation_orders = []
    for order in TRANSFORMATION_ORDERS.values():
        transformation_order = build_transformation_order(order)
        add_order = (not workflow_id or (workflow_id == order["WorkflowId"])) and (
            not status or (status == transformation_order["Status"])
        )
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
            f"{list(missing_options_values)} are mandatory options for workflow {workflow_id}, "
            f"but they are missing in order definition "
        )
    return workflow_options


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
    is used the value of the environment variable "HUBS_CREDENTIALS_FILE"
    :param str scheduler:  optional the scheduler to be used fot the client instantiation. If it is None it will be used
    the value of environment variable SCHEDULER.
    """

    workflow = get_workflows().get(workflow_id)
    if workflow is None:
        raise ValueError(f"Workflow id {workflow_id} not found.")
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

    if order_id in TRANSFORMATION_ORDERS:
        order = TRANSFORMATION_ORDERS[order_id]
        future = order["future"]
        if future.status == "error":
            client.retry(future)
            order["SubmissionDate"] = datetime.now().isoformat()
            order.pop("CompletedDate", None)
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
        future.add_done_callback(add_completed_date)

    return build_transformation_order(order)
