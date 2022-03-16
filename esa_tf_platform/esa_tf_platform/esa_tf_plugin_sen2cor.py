import glob
import logging
import os
import subprocess
from xml.etree import ElementTree

import pkg_resources

SEN2COR_CONFILE_NAME = "L2A_GIPP.xml"
SRTM_DOWNLOAD_ADDRESS = (
    "http://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/"
)
MTD_FILENAME = "MTD_MSIL1C.xml"
ROI_OPTIONS_NAMES = {"row0", "col0", "nrow_win", "ncol_win"}

OZONE_WINTER_VALUES = (0, 250, 290, 330, 377, 420, 460)
OZONE_SUMMER_VALUES = (0, 250, 290, 331, 370, 410, 450)

DEFAULT_USER = "no_user"

logger = logging.getLogger(__name__)


def set_sen2cor_options(etree, options, srtm_dir):
    """Replace in the input ElementTree object (representing the parsed default L2A_GIPP.xml
    configuration file) the values of tags that a user can specify as processing options, according
    to the user desiderata specified by means of ``options``. The ``srtm_path`` is the system
    folder in which the SRTM DEM will be downloaded or, if DEM is already available, searched.

    :param ElementTree.ElementTree etree: the parsed default L2A_GIPP.xml configuration file
    :param dict options: dictionary of the user options
    :param str srtm_dir: path of the folder in which the SRTM DEM will be downloaded or searched
    :return ElementTree.ElementTree:
    """
    for k, v in options.items():
        if k != "Resolution":
            etree.findall(f".//{k}")[0].text = str(v).upper()
        if srtm_dir:
            etree.findall(".//DEM_Directory")[0].text = srtm_dir
            etree.findall(".//DEM_Reference")[0].text = SRTM_DOWNLOAD_ADDRESS
    return etree


def create_sen2cor_confile(processing_dir, srtm_path, options):
    """Parse the default Sen2Cor L2A_GIPP.xml configuration file, set the user options and write
    the new configuration file relative to the current customisation in the processing-dir. The
    function returns the full path of the new created L2A_GIPP.xml file.

    :param str processing_dir: path of the processing directory
    :param str srtm_path: path of the folder in which the SRTM DEM will be downloaded
    :param dict options: dictionary of the user options
    :return str:
    """
    # Read the default Sen2Cor configuration file
    sample_config_path = pkg_resources.resource_filename(
        __package__, os.path.join("resources", SEN2COR_CONFILE_NAME)
    )
    et = ElementTree.parse(sample_config_path)
    et = set_sen2cor_options(et, options, srtm_path)
    # Write back to file
    output_confile = os.path.join(processing_dir, SEN2COR_CONFILE_NAME)
    et.write(output_confile)
    return os.path.abspath(output_confile)


def check_input_consistency(product_folder_path):
    """Check if the input product is a valid Sentinel-2 L1C product.

    :param str product_folder_path: path of the main Sentinel-2 L1C product folder
    :return:
    """
    msg = (
        f"the input product {product_folder_path} is not a valid Sentinel-2 L1C product"
    )
    if not os.path.isdir(product_folder_path):
        raise ValueError(msg)
    mtd_file = glob.glob(os.path.join(product_folder_path, MTD_FILENAME))
    if len(mtd_file) != 1:
        raise ValueError(msg)
    img_data = glob.glob(
        os.path.join(product_folder_path, "GRANULE", "*", "IMG_DATA", "*.jp2")
    )
    if len(img_data) != 14:
        raise ValueError(msg)


def check_ozone_content(options):
    """Check the validity of the ozone content option values.

    :param dict options: dictionary of the asked user options
    :return bool:
    """
    ozone_content_value = options["Ozone_Content"]
    mid_latitude_value = options.get(
        "Mid_Latitude", sen2cor_l1c_l2a["WorkflowOptions"]["Mid_Latitude"]["Default"]
    )
    valid_ozone_values = sen2cor_l1c_l2a["WorkflowOptions"]["Ozone_Content"]["Enum"]
    if ozone_content_value not in valid_ozone_values:
        raise ValueError(
            f"valid ozone content values are {valid_ozone_values}, given {ozone_content_value}"
        )
    if mid_latitude_value == "WINTER" and (
        ozone_content_value not in OZONE_WINTER_VALUES
    ):
        raise ValueError(
            f"when 'Mid_Latitude=WINTER' the allowed ozone content values are {OZONE_WINTER_VALUES}"
        )
    elif mid_latitude_value == "SUMMER" and (
        ozone_content_value not in OZONE_SUMMER_VALUES
    ):
        raise ValueError(
            f"when 'Mid_Latitude=SUMMER' the allowed ozone content values are {OZONE_SUMMER_VALUES}"
        )


def check_row_col_type(roi_options):
    """Check the validity of the ROI options ``row0``, ``col0``.

    :param dict roi_options: the ROI options ``row0``, ``col0``, ``nrow_win`` and ``ncol_win``
    :return bool:
    """
    # if row0 is a string, col0 must be a string with the same value
    if isinstance(roi_options["row0"], str):
        if not isinstance(roi_options["col0"], str):
            raise ValueError("row0 and col0 must be or two string or two integers")
        if roi_options["row0"] != roi_options["col0"]:
            raise ValueError("if row0 and col0 values are string, they must be equal")
    # if row0 is an integer, then, also col0 must be an integer
    if isinstance(roi_options["row0"], int):
        if not isinstance(roi_options["col0"], int):
            raise ValueError("row0 and col0 must be or two string or two integers")
        for oname in ["row0", "col0"]:
            if roi_options[oname] % 6 != 0:
                raise ValueError("row0, col0 must be integer divisible by 6")


def check_nrow_ncol(roi_options):
    """Check the validity of the ROI options ``nrow_win`` and ``ncol_win``.

    :param dict roi_options: the ROI options ``row0``, ``col0``, ``nrow_win`` and ``ncol_win``
    :return bool:
    """
    for oname in ["nrow_win", "ncol_win"]:
        if roi_options[oname] % 6 != 0:
            raise ValueError("nrow_win and ncol_win must be integer divisible by 6")


def check_roi_options(roi_options):
    """Check the validity of the ROI options.

    :param dict roi_options: the ROI options ``row0``, ``col0``, ``nrow_win`` and ``ncol_win``
    :return bool:
    """
    if not roi_options:
        return
    # if ROI options are present, they must be 4, not just a few
    if len(roi_options) != len(ROI_OPTIONS_NAMES):
        missing_options = ROI_OPTIONS_NAMES.difference(set(roi_options.keys()))
        raise ValueError(f"some ROI options are missing: {missing_options}")
    check_row_col_type(roi_options)
    check_nrow_ncol(roi_options)


def check_options(options):
    """Check the validity of the asked user options.

    :param dict options: the user's options dictionary
    :return bool:
    """
    # ATM ROI options are not present in the workflow definition
    roi_options = {k: v for k, v in options.items() if k in ROI_OPTIONS_NAMES}
    check_roi_options(roi_options)

    # check the validity of non-ROI options
    other_options = {k: v for k, v in options.items() if k not in ROI_OPTIONS_NAMES}
    valid_names = list(sen2cor_l1c_l2a["WorkflowOptions"])
    for oname, ovalue in other_options.items():
        if oname == "Ozone_Content":
            check_ozone_content(options)
        elif oname in valid_names:
            if oname == "Resolution" and ovalue is None:
                continue
            valid_values = sen2cor_l1c_l2a["WorkflowOptions"][oname].get("Enum")
            if valid_values and (ovalue not in valid_values):
                raise ValueError(
                    f"invalid value '{ovalue}'' for '{oname}': valid values are {valid_values}"
                )
        else:
            raise ValueError(f"invalid option {oname}: valid options are {valid_names}")


def log_options(workflow_options, user_id=DEFAULT_USER):
    """Print the required Sen2Cor options (user desiderata + default values).

    :param workflow_options: the user's options dictionary
    :param str user_id: user identifier
    :return dict:
    """
    applied_options = {
        option_name: option.get("Default")
        for option_name, option in sen2cor_l1c_l2a["WorkflowOptions"].items()
    }
    applied_options.update(workflow_options)
    logger.info(applied_options, extra=dict(user=user_id))
    return applied_options


def find_output(output_dir):
    """Return the output product path.

    :param str output_dir: the folder path in which the Sen2Cor output has been saved
    :return str:
    """
    sen2cor_output = None
    # cycling over output_dir content to discard file like .DS_STORE
    for d in os.listdir(output_dir):
        if os.path.isdir(os.path.join(output_dir, d)) and (not d.startswith(".")):
            sen2cor_output = d
            break
    if sen2cor_output is None:
        raise RuntimeError(
            f"no Sen2Cor output product dir has been found in {output_dir}"
        )
    output_path = os.path.join(output_dir, sen2cor_output)
    return output_path


def run_command(cmd, processing_dir, user_id=DEFAULT_USER):
    """Execute a Sen2Cor command line in a new process. The Sen2Cor standard output is read during
    the processing, then it is sent both to a dedicated log-file and to the server as log messages.
    The function returns the exit code of the Sen2Cor sub-process and the path of the Sen2Cor
    log-file saved in the processing directory.

    :param str cmd: Sen2Cor command line must be executed
    :param str processing_dir: path fo the processing directory
    :param str user_id: user_identifier
    :return (str, str):
    """
    logger.info(
        f"\nthe following Sen2Cor command will be executed:\n    {cmd}\n",
        extra=dict(user=user_id),
    )
    sen2cor_log_path = os.path.join(processing_dir, "sen2cor_log.log")
    process = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True
    )
    with open(sen2cor_log_path, "w", buffering=1) as f_log:
        while process.poll() is None:
            lines = iter(process.stdout.readline, b"")
            try:
                line = next(lines)
            except:
                continue
            f_log.write(line)
            logger.info(line, extra=dict(user=user_id))
    process.stdout.close()
    exit_status = process.returncode
    if exit_status != 0:
        raise subprocess.CalledProcessError(exit_status, cmd)
    return exit_status, sen2cor_log_path


def run_processing(
    product_path,
    *,
    workflow_options,
    processing_dir,
    output_dir,
    sen2cor_script_file=None,
    srtm_dir=None,
    user_id=DEFAULT_USER,
):
    """Execute the processing by means of Sen2Cor tool to convert, according to the input user
    option, an input Sentinel-2 L1C product into a Sentinel-2 L2A product. The function returns
    the path of the output product.

    :param str product_path: path of the main Sentinel-2 L1C product folder
    :param dict workflow_options: the user's options dictionary
    :param str processing_dir: path of the processing directory
    :param str output_dir: the output directory
    :param str sen2cor_script_file: path of the Sen2Cor ``L2A_Process`` script. If not defined the
    environment variable ``SEN2COR_SCRIPT_FILE`` will be used.
    :param str srtm_dir: path of the folder in which the SRTM DEM will be downloaded or searched. If not defined the
    environment variable ``SRTM_DIR`` will be used. In case this variable does not exist a directory dem
    in the ``processing_dir`` will be created.
    :param str user_id: user identifier
    :return str:
    """
    if sen2cor_script_file is None:
        sen2cor_script_file = os.getenv("SEN2COR_SCRIPT_FILE", "L2A_Process")
    if srtm_dir is None:
        srtm_dir = os.getenv("SRTM_DIR", None)
    output_dir = os.path.abspath(output_dir)
    processing_dir = os.path.abspath(processing_dir)
    product_path = os.path.abspath(product_path)
    # if the "srtm_path" is not defined, the SRTM tile is downloaded inside a dedicate folder
    # into the processing-dir
    if not srtm_dir:
        srtm_dir = os.path.join(processing_dir, "dem")
        os.makedirs(srtm_dir, exist_ok=True)
    else:
        srtm_dir = os.path.abspath(srtm_dir)
    if srtm_dir and not os.path.isdir(srtm_dir):
        raise ValueError(
            f"{srtm_dir} not found, please define it using the environment variable 'SRTM_DIR'"
        )

    check_input_consistency(product_path)
    check_options(workflow_options)
    log_options(workflow_options, user_id)

    # creation of the Sen2Cor configuration files inside the processing-dir
    sen2cor_confile = create_sen2cor_confile(processing_dir, srtm_dir, workflow_options)
    # running the Sen2Cor script
    cmd = f"{sen2cor_script_file} {product_path} --output_dir {output_dir} --GIP_L2A {sen2cor_confile}"
    if workflow_options.get("Resolution"):
        cmd += f" --resolution {workflow_options.get('Resolution')}"
    run_command(cmd, processing_dir, user_id)
    output_path = find_output(output_dir)
    return output_path


sen2cor_l1c_l2a = {
    "WorkflowName": "Sen2Cor_L1C_L2A",
    "Description": "Product processing from Sentinel-2 L1C to L2A using Sen2Cor v2.10, supporting Level-1C product version 14.2 - 14.9",
    "Execute": "esa_tf_platform.esa_tf_plugin_sen2cor.run_processing",
    "InputProductType": "S2MSI1C",
    "OutputProductType": "S2MSI2A",
    "WorkflowVersion": "0.2",
    "WorkflowOptions": {
        "Aerosol_Type": {
            "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
            "Type": "string",
            "Default": "RURAL",
            "Enum": ["MARITIME", "RURAL"],
        },
        "Mid_Latitude": {
            "Description": "If 'AUTO' the atmosphere profile will be determined automatically by the processor, selecting WINTER or SUMMER atmosphere profile based on the acquisition date and geographic location of the tile",
            "Type": "string",
            "Default": "SUMMER",
            "Enum": ["SUMMER", "WINTER", "AUTO"],
        },
        "Ozone_Content": {
            "Description": "0: to get the best approximation from metadata (this is the smallest difference between metadata and column DU), else select for midlatitude summer (MS) atmosphere: 250, 290, 331 (standard MS), 370, 410, 450; for midlatitude winter (MW) atmosphere: 250, 290, 330, 377 (standard MW), 420, 460",
            "Type": "integer",
            "Default": 331,
            "Enum": [0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460],
        },
        "Cirrus_Correction": {
            "Description": "FALSE: no cirrus correction applied, TRUE: cirrus correction applied",
            "Type": "boolean",
            "Default": False,
            "Enum": [True, False],
        },
        "DEM_Terrain_Correction": {
            "Description": "Use DEM for Terrain Correction, otherwise only used for WVP and AOT",
            "Type": "boolean",
            "Default": True,
            "Enum": [True, False],
        },
        "Resolution": {
            "Description": "Target resolution, can be 10, 20 or 60m. If omitted, 10, 20 and 60m resolutions will be processed",
            "Type": "integer",
            "Default": None,
            "Enum": [10, 20, 60],
        },
    },
}
