import itertools
import os
import warnings

import pkg_resources
import sentinelsat
import yaml


def remove_duplicates(pkg_entrypoints):
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
            warnings.warn(
                f"Found {matches_len} entrypoints for the workflow name {name}:"
                f"\n {all_module_names}.\n It will be used: {selected_module_name}.",
                RuntimeWarning,
            )
    return unique_pkg_entrypoints


def workflow_dict_from_pkg(pkg_entrypoints):
    workflow_entrypoints = {}
    for pkg_ep in pkg_entrypoints:
        name = pkg_ep.name
        try:
            workflow_config = pkg_ep.load()
            workflow_entrypoints[name] = workflow_config
        except Exception as ex:
            warnings.warn(f"Workflow {name!r} loading failed:\n{ex}", RuntimeWarning)
    return workflow_entrypoints


def load_workflows_configurations(pkg_entrypoints):
    pkg_entrypoints = remove_duplicates(pkg_entrypoints)
    workflow_entrypoints = workflow_dict_from_pkg(pkg_entrypoints)
    return {
        name: {**workflows, "Id": name}
        for name, workflows in workflow_entrypoints.items()
    }


def filter_by_product_type(workflows, product_type=None):
    filtered_workflows = {}
    for name in workflows:
        if product_type == workflows[name]["InputProductType"]:
            filtered_workflows[name] = workflows[name]
    return filtered_workflows


def get_workflows(product_type=None):
    """
    Returns the list of available workflows that can process a 'product_type' products.
    """
    pkg_entrypoints = pkg_resources.iter_entry_points("esa_tf.plugin")
    workflows = load_workflows_configurations(pkg_entrypoints)
    if product_type:
        workflows = filter_by_product_type(workflows, product_type)
    return workflows


def get_workflow_by_id(workflow_id=None):
    workflows = get_workflows()
    try:
        workflow = workflows[workflow_id]
    except KeyError:
        raise ValueError(
            f"Workflow {workflow_id} not found, available workflows are {list(workflows.keys())}"
        )
    return workflow


def create_directories(*directory_list):
    for directory in directory_list:
        if not os.path.exists(directory):
            os.makedirs(directory)


def read_hub_credentials(
    hubs_credential_file,
    hub_name="scihub",
):
    with open(hubs_credential_file) as file:
        hubs_credentials = yaml.load(file, Loader=yaml.FullLoader)
    return hubs_credentials[hub_name]


def download_product(
    product,
    *,
    processing_dir,
    hubs_credentials_file,
    hub_name="scihub",
):
    hub_credentials = read_hub_credentials(hubs_credentials_file, hub_name)
    api = sentinelsat.SentinelAPI(**hub_credentials)
    uuid_products = api.query(identifier=product.strip(".zip"))
    if len(uuid_products) == 0:
        raise ValueError(f"{product} not found in hub: {hub_credentials['api_url']}")
    if len(uuid_products) > 1:
        raise ValueError(
            f"for {product} multiple uuid found: {list(uuid_products.keys())}"
        )
    uuid_product = list(uuid_products.keys())[0]
    product_info = api.download(
        uuid_product, directory_path=processing_dir, checksum=True, nodefilter=None
    )
    return product_info["path"]


def run_workflow(
    workflow_id,
    *,
    product_reference,
    workflow_options,
    order_id,
    working_dir=None,
    output_dir=None,
    hubs_credentials_file=None,
):
    """
    Run the workflow defined by 'workflow_id':
    :param str workflow_id:  id that identifies the workflow to run,
    :param dict product_reference: dictionary containing the information to retrieve the product to be processed
    ('Reference', i.e. product name and 'api_hub', i.e. name of the ub where to download the data), e.g.:
    {'Reference': 'S2A_MSIL1C_20170205T105221_N0204_R051_T31TCF_20170205T105426', 'api_hub', 'scihub'}.
    :param dict workflow_options: dictionary cotaining the workflow kwargs.
    :param str order_id: unique identifier of the processing order, used to create a processing folder
    :param str working_dir: optional working directory where will be create the processing directory. If it is None
    it is used the value of the environment variable "WORKING_DIR".
    :param str output_dir: optional output directory. If it is None it is used the value of the environment
    variable "OUTPUT_DIR"
    :param str hubs_credentials_file:  optional file containing the credential of the hub. If it is None it
    is used the value of the environment variable "HUBS_CREDENTIALS_FILE"
    """
    # define create directories
    if working_dir is None:
        working_dir = os.getenv("WORKING_DIR", "./working_dir")
    if output_dir is None:
        output_dir = os.getenv("OUTPUT_DIR", "./output_dir")
    if hubs_credentials_file is None:
        hubs_credentials_file = os.getenv(
            "HUBS_CREDENTIALS_FILE", "./hubs_credential.yaml"
        )
    if not os.path.isfile(hubs_credentials_file):
        raise ValueError(
            f"{hubs_credentials_file} not not found, please define it using 'hubs_credentials_file' "
            "keyword argument or the environment variable HUBS_CREDENTIAL_FILE"
        )
    processing_dir = os.path.join(working_dir, order_id)
    create_directories(working_dir, output_dir, processing_dir)

    # download
    product = product_reference["Reference"]
    hub_name = product_reference.get("api_hub", "scihub")
    product_file = download_product(
        product=product,
        hubs_credentials_file=hubs_credentials_file,
        processing_dir=processing_dir,
        hub_name=hub_name,
    )

    # run workflow
    workflow_runner = get_workflow_by_id(workflow_id)["Execute"]
    output_file = workflow_runner(
        product_file,
        processing_dir=processing_dir,
        output_dir=output_dir,
        workflow_options=workflow_options
    )

    # delite workflow processing dir
    os.rmdir(processing_dir)
    return output_file
