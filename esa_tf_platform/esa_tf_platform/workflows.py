import importlib
import itertools
import logging
import os
import shutil
import zipfile

import dask.distributed
import pkg_resources
import sentinelsat
import yaml

logger = logging.getLogger(__name__)

TYPES = {
    "boolean": bool,
    "number": float,
    "integer": int,
    "string": str,
}

MANDATORY_WORKFLOWS_KEYS = [
    "WorkflowName",
    "Description",
    "Execute",
    "InputProductType",
    "OutputProductType",
    "WorkflowVersion",
    "WorkflowOptions",
]

MANDATORY_OPTIONS_KEYS = [
    "Description",
    "Type",
]


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


def check_valid_product_type(workflow, workflow_id=None):
    product_type = workflow["InputProductType"]
    if (product_type not in SENTINEL1) and (product_type not in SENTINEL2):
        raise ValueError(
            f"error in workflow plugin {workflow_id}: product type {product_type} not recognized; "
            f"product type shall be one of the following {[*SENTINEL1, *SENTINEL2]}"
        )


def check_mandatory_workflow_keys(workflow, workflow_id=None):
    """
    Check if all mandatory keys are in the workflow.
    :param dict workflow: workflow configuration dictionary
    :param str workflow_id: workflow is needed for the error message
    """

    for key in MANDATORY_WORKFLOWS_KEYS:
        if key not in workflow:
            raise ValueError(
                f"workflow_id {workflow_id}: missing key {key} "
                f"in workflow description"
            )


def check_mandatory_option_keys(worflow_options, workflow_id=None):
    """
    Check if all mandatory keys are in the workflow option dictionary.
    :param dict worflow_options: "WorkflowOptions" configuration dictionary
    :param str workflow_id: workflow is needed for the error message
    """
    for option_name, option in worflow_options.items():
        for key in MANDATORY_OPTIONS_KEYS:
            if key not in option:
                raise ValueError(
                    f"workflow_id {workflow_id}: missing key "
                    f"{key} in {workflow_id} workflow description"
                )


def check_valid_declared_type(worflow_options, workflow_id=None):
    """
    Check if the declared type option is a valid one.
    :param dict worflow_options: "WorkflowOptions" configuration dictionary
    :param str workflow_id: workflow is needed for the error message
    """
    for option_name, option in worflow_options.items():
        option_type = option["Type"]
        if option_type not in TYPES:
            raise ValueError(
                f"workflow_id {workflow_id}: {option_type} type in {option_name} "
                f"not recognized. The type shall be one of the following: {TYPES}"
            )


def check_default_type(worflow_options, workflow_id=None):
    """
    Check if option default type is in line with declared option type.
    :param dict worflow_options: "WorkflowOptions" configuration dictionary
    :param str workflow_id: workflow is needed for the error message
    """

    for option_name, option in worflow_options.items():
        default = option.get("Default", None)
        if default is None:
            continue
        option_type = option["Type"]
        if not isinstance(default, TYPES[option_type]):
            raise ValueError(
                f"workflow_id {workflow_id}: {option_name} Default {default} type is not align "
                f"with declared type {option_type}"
            )


def check_enum_type(worflow_options, workflow_id=None):
    """
    Check if option enum type is in line with declared option type.
    :param dict worflow_options: "WorkflowOptions" configuration dictionary
    :param str workflow_id: workflow is needed for the error message
    """

    for option_name, option in worflow_options.items():
        enum = option.get("Enum")
        if enum is None:
            continue
        option_type = option["Type"]

        for value in enum:
            if not isinstance(value, TYPES[option_type]):
                raise ValueError(
                    f"workflow_id {workflow_id}: {option_name} Enum value "
                    f"{value} type is not align with declared type {option_type}"
                )


def check_workflow(workflow, workflow_id=None):
    """
    Check if workflow keys, options keys and types.
    :param dict workflow: workflow configuration dictionary
    :param str workflow_id: workflow is needed for the error message
    """
    # Note: the order of the checks can't be modified
    check_mandatory_workflow_keys(workflow, workflow_id=workflow_id)
    check_valid_product_type(workflow, workflow_id=workflow_id)
    check_mandatory_option_keys(workflow["WorkflowOptions"], workflow_id=workflow_id)
    check_valid_declared_type(workflow["WorkflowOptions"], workflow_id=workflow_id)
    check_default_type(workflow["WorkflowOptions"], workflow_id=workflow_id)
    check_enum_type(workflow["WorkflowOptions"], workflow_id=workflow_id)


def remove_duplicates(pkg_entrypoints):
    """
    Remove entrypoints with the same name, keeping only the first one.
    """
    # sort and group entrypoints by name
    pkg_entrypoints = sorted(pkg_entrypoints, key=lambda ep: ep.name)
    pkg_entrypoints_grouped = itertools.groupby(pkg_entrypoints, key=lambda ep: ep.name)
    # check if there are multiple entrypoints for the same name
    unique_pkg_entrypoints = []
    for name, matches in pkg_entrypoints_grouped:
        matches = list(matches)
        unique_pkg_entrypoints.append(matches[0])
        matches_len = len(matches)
        if matches_len > 1:
            selected_module_name = matches[0].module_name
            all_module_names = [e.module_name for e in matches]
            logging.warning(
                f"found {matches_len} entrypoints for the workflow name {name}:"
                f"\n {all_module_names}.\n It will be used: {selected_module_name}."
            )
    return unique_pkg_entrypoints


def workflow_dict_from_pkg(pkg_entrypoints):
    """
    Load the entrypoints and store them in a dictionary
    """
    workflow_entrypoints = {}
    for pkg_ep in pkg_entrypoints:
        name = pkg_ep.name
        try:
            workflow_config = pkg_ep.load()
            workflow_entrypoints[name] = workflow_config
        except Exception:
            logger.exception(f"workflow {name!r} registration failed with error:")
    return workflow_entrypoints


def load_workflows_configurations(pkg_entrypoints):
    """
    Create the dictionary containing all the workflows configuration installed
    """
    pkg_entrypoints = remove_duplicates(pkg_entrypoints)
    workflow_entrypoints = workflow_dict_from_pkg(pkg_entrypoints)
    return {
        name: {**workflows, "Id": name}
        for name, workflows in workflow_entrypoints.items()
    }


def get_all_workflows():
    """
    Return the list of all available workflows.
    """
    pkg_entrypoints = pkg_resources.iter_entry_points("esa_tf.plugin")
    workflows = load_workflows_configurations(pkg_entrypoints)
    valid_workflows = {}
    for workflow_id, workflow in workflows.items():
        try:
            check_workflow(workflow, workflow_id=workflow_id)
            valid_workflows[workflow_id] = workflow
        except ValueError:
            logger.exception(
                f"workflow {workflow_id} registration failed; error in workflow description:"
            )
    return valid_workflows


def read_hub_credentials(hubs_credential_file,):
    """
    Read credentials from the hubs_credential_file.
    """
    with open(hubs_credential_file) as file:
        hubs_credentials = yaml.load(file, Loader=yaml.FullLoader)
    return hubs_credentials


def download_product_from_hub(
    product, *, processing_dir, hub_credentials,
):
    """
    Download the product from the selected hub
    """
    api = sentinelsat.SentinelAPI(**hub_credentials)
    uuid_products = api.query(identifier=product.strip(".zip"))
    if len(uuid_products) == 0:
        raise ValueError(f"{product} not found in hub: {hub_credentials['api_url']}")
    uuid_product = list(uuid_products)[0]
    product_info = api.download(
        uuid_product, directory_path=processing_dir, checksum=True, nodefilter=None
    )
    return product_info["path"]


def download_product(
    product, *, processing_dir, hubs_credentials_file, hub_name=None, order_id=None,
):
    """
    Download the product from the first hub in the hubs_credentials_file that publishes the product
    """
    hubs_credentials = read_hub_credentials(hubs_credentials_file)
    if hub_name:
        hubs_credentials = {hub_name: hubs_credentials[hub_name]}
    product_path = None
    for hub_name in hubs_credentials:
        try:
            product_path = download_product_from_hub(
                product,
                processing_dir=processing_dir,
                hub_credentials=hubs_credentials[hub_name],
            )
        except Exception:
            logger.exception(
                f"not able to download from {hub_name}, an error occurred:",
            )
        if product_path:
            break
    if product_path is None:
        raise ValueError(
            f"order_id {order_id}: could not download product from {list(hubs_credentials)}"
        )
    return product_path


def unzip_product(product_zip_file, processing_dir):
    """
    Unzip the product in the processing dir

    :param str product_zip_file: path to product zip file
    :param str processing_dir: directory where to unzip the product zip
    """
    with zipfile.ZipFile(product_zip_file, "r") as product_zip:
        product_folder = product_zip.infolist()[0].filename
        product_zip.extractall(processing_dir)
    return os.path.join(processing_dir, product_folder)


def zip_product(output, output_dir):
    """Zip the workflow output folder and return the zip file path.

    :param str output: full path of the workflow output folder
    :param str output_dir: path of the folder in which the zip file will be created
    :return str:
    """
    basename = os.path.basename(output.rstrip("/"))
    dirname = os.path.dirname(output.rstrip("/"))
    # remove the ".SAFE" string (if present) from the workflow output folder
    zip_basename = basename.rsplit(".SAFE")[0]
    output_zip_path = os.path.join(output_dir, zip_basename)
    output_file = shutil.make_archive(
        base_name=output_zip_path, format="zip", root_dir=dirname, base_dir=basename,
    )
    return output_file


def load_workflow_runner(workflow_id):
    """Loads workflow runner function
    :param str workflow_id: workflow-ID
    """
    # run workflow
    workflow_runner_name = get_all_workflows()[workflow_id]["Execute"]
    module_name, function_name = workflow_runner_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    workflow_runner = getattr(module, "run_processing")
    return workflow_runner


def run_workflow(
    workflow_id,
    *,
    product_reference,
    workflow_options,
    order_id,
    working_dir=None,
    output_dir=None,
    hubs_credentials_file=None,
    output_owner=-1,
):
    """
    Run the workflow defined by 'workflow_id':
    :param str workflow_id:  id that identifies the workflow to run,
    :param dict product_reference: dictionary containing the information to retrieve the product to be processed
    ('Reference', i.e. product name and 'api_hub', i.e. name of the ub where to download the data), e.g.:
    {'Reference': 'S2A_MSIL1C_20170205T105221_N0204_R051_T31TCF_20170205T105426', 'api_hub', 'scihub'}.
    :param dict workflow_options: dictionary cotaining the workflow kwargs.
    :param str order_id: unique identifier of the processing order, used to create a processing folder
    :param str working_dir: optional working directory where will be create the processing directory. If it is None,
    the environment variable ``WORKING_DIR`` is used.
    :param str output_dir: optional output directory. If it is None, the environment variable ``OUTPUT_DIR`` is used.
    :param str hubs_credentials_file:  optional file containing the credential of the hub. If it is None,
    the environment variable ``HUBS_CREDENTIALS_FILE`` is used.
    :param output_owner:
    """
    # define create directories
    try:
        dask_worker = dask.distributed.worker.get_worker()
        logger.info(f"start processing on worker: {dask_worker.name!r}",)
    except ValueError:
        pass

    if working_dir is None:
        working_dir = os.getenv("WORKING_DIR", "./working_dir")
    if output_dir is None:
        output_dir = os.getenv("OUTPUT_DIR", "./output_dir")
    if output_owner == -1:
        output_owner = int(os.getenv("OUTPUT_OWNER_ID", "-1"))
    if hubs_credentials_file is None:
        hubs_credentials_file = os.getenv(
            "HUBS_CREDENTIALS_FILE", "./hubs_credentials.yaml"
        )
    if not os.path.isfile(hubs_credentials_file):
        raise ValueError(
            f"{hubs_credentials_file} not found, please define it using 'hubs_credentials_file' "
            "keyword argument or the environment variable HUBS_CREDENTIALS_FILE"
        )
    processing_dir = os.path.join(working_dir, order_id)
    output_binder_dir = os.path.join(working_dir, order_id, "output_binder_dir")
    for directory in [working_dir, output_dir, processing_dir, output_binder_dir]:
        os.makedirs(directory, exist_ok=True)

    # download
    product = product_reference["Reference"]
    hub_name = product_reference.get("DataSourceName")
    logger.info(f"downloading input product: {product!r}")
    product_zip_file = download_product(
        product=product,
        hubs_credentials_file=hubs_credentials_file,
        processing_dir=processing_dir,
        hub_name=hub_name,
        order_id=order_id,
    )
    logger.info(f"unpack input product: {product_zip_file!r}")
    product_path = unzip_product(product_zip_file, processing_dir)

    # run workflow
    workflow_runner = load_workflow_runner(workflow_id)

    logger.info(f"run workflow: {workflow_id!r}, {workflow_options!r}")
    output = workflow_runner(
        product_path,
        processing_dir=processing_dir,
        output_dir=output_binder_dir,
        workflow_options=workflow_options,
    )

    # re-package the output
    logger.info(f"package output product: {output!r}")

    output_order_dir = os.path.join(output_dir, order_id)
    os.makedirs(output_order_dir, exist_ok=True)
    output_zip_file = zip_product(output, output_order_dir)
    shutil.chown(output_zip_file, user=output_owner)

    # delete workflow processing dir
    shutil.rmtree(processing_dir, ignore_errors=True)
    return os.path.join(order_id, os.path.basename(output_zip_file))
