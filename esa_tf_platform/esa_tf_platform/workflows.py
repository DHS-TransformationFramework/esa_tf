import importlib
import itertools
import logging
import os
import shutil
import warnings
import zipfile

import pkg_resources
import sentinelsat
import yaml

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
            warnings.warn(
                f"found {matches_len} entrypoints for the workflow name {name}:"
                f"\n {all_module_names}.\n It will be used: {selected_module_name}.",
                RuntimeWarning,
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
        except Exception as ex:
            warnings.warn(f"workflow {name!r} loading failed:\n{ex}", RuntimeWarning)
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
    return workflows


def read_hub_credentials(
    hubs_credential_file,
):
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


def download_product(product, *, processing_dir, hubs_credentials_file, hub_name=None):
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
                hub_credentials=hubs_credentials[hub_name]
            )
        except Exception as ex:
            print(f"{ex.msg} {ex.response}")
    if product_path is None:
        raise ValueError(f"could not download product from {list(hubs_credentials)}")
    return product_path


def unzip_product(product_zip_file, processing_dir):
    """
    Unzip the product in the processing dir
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
    """
    # define create directories
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
            f"{hubs_credentials_file} not not found, please define it using 'hubs_credentials_file' "
            "keyword argument or the environment variable HUBS_CREDENTIALS_FILE"
        )
    processing_dir = os.path.join(working_dir, order_id)
    output_binder_dir = os.path.join(working_dir, order_id, "output_binder_dir")
    for directory in [working_dir, output_dir, processing_dir, output_binder_dir]:
        os.makedirs(directory, exist_ok=True)

    # download
    product = product_reference["Reference"]
    hub_name = product_reference.get("DataSourceName")
    product_zip_file = download_product(
        product=product,
        hubs_credentials_file=hubs_credentials_file,
        processing_dir=processing_dir,
        hub_name=hub_name,
    )
    product_path = unzip_product(product_zip_file, processing_dir)

    # run workflow
    workflow_runner_name = get_all_workflows()[workflow_id]["Execute"]
    module_name, function_name = workflow_runner_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    workflow_runner = getattr(module, "run_processing")

    output = workflow_runner(
        product_path,
        processing_dir=processing_dir,
        output_dir=output_binder_dir,
        workflow_options=workflow_options,
    )

    # re-package the ouput
    output_zip_file = zip_product(output, output_dir)
    shutil.chown(output_zip_file, user=output_owner)

    # delete workflow processing dir
    shutil.rmtree(processing_dir, ignore_errors=True)
    return output_zip_file
