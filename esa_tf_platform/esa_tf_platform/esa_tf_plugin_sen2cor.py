import glob
import logging
import os
from xml.etree import ElementTree
import subprocess


SEN2COR_CONFILE_NAME = "L2A_GIPP.xml"
SRTM_DOWNLOAD_ADDRESS = (
    "http://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/"
)
MTD_FILENAME = "MTD_MSIL1C.xml"
ROI_OPTIONS_NAMES = {"row0", "col0", "nrow_win", "ncol_win"}

sen2cor_l1c_l2a = {
    "Description": "Product processing from Sentinel-2 L1C to L2A. Processor V2.3.6",
    "InputProductType": "S2MSILC",
    "OutputProductType": "S2MSI2A",
    "WorkflowVersion": "0.1",
    "WorkflowOptions": [
        {
            "Name": "aerosol_type",
            "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
            "Type": "string",
            "Default": "rural",
            "Values": ["maritime", "rural"],
        },
        {
            "Name": "mid_latitude",
            "Description": "If  'AUTO' the atmosphere profile will be determined automatically by the processor, selecting WINTER or SUMMER atmosphere profile based on the acquisition date and geographic location of the tile",
            "Type": "string",
            "Default": "summer",
            "Values": ["summer", "winter", "auto"],
        },
        {
            "Name": "ozone_content",
            "Description": "0: to get the best approximation from metadata (this is the smallest difference between metadata and column DU), else select for midlatitude summer (MS) atmosphere: 250, 290, 331 (standard MS), 370, 410, 450; for midlatitude winter (MW) atmosphere: 250, 290, 330, 377 (standard MW), 420, 460",
            "Type": "integer",
            "Default": 331,
            "Values": [0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460],
        },
        {
            "Name": "cirrus_correction",
            "Description": "FALSE: no cirrus correction applied, TRUE: cirrus correction applied",
            "Type": "boolean",
            "Default": False,
            "Values": [True, False],
        },
        {
            "Name": "dem_terrain_correction",
            "Description": "Use DEM for Terrain Correction, otherwise only used for WVP and AOT",
            "Type": "boolean",
            "Default": True,
            "Values": [True, False],
        },
        {
            "Name": "resolution",
            "Description": "Target resolution, can be 10, 20 or 60m. If omitted, 10, 20 and 60m resolutions will be processed",
            "Type": "boolean",
            "Default": True,
            "Values": [10, 20, 60],
        },
    ],
}


def set_sen2cor_options(etree, options, srtm_path):
    """Replace in the input ElementTree object (representing the parsed default L2A_GIPP.xml
    configuration file) the values of tags that a user can specify as processing options, according
    to the user desiderata specified by means of ``options``. The ``srtm_path`` is the system
    folder in which the SRTM DEM will be downloaded or, if DEM is already available, searched.

    :param ElementTree.ElementTree etree: the parsed default L2A_GIPP.xml configuration file
    :param dict options: dictionary of the user options
    :param str srtm_path: path of the folder in which the SRTM DEM will be downloaded or searched
    :return ElementTree.ElementTree:
    """
    for k, v in options.items():
        if k.lower() in ["row0", "col0", "nrow_win", "ncol_win"]:
            etree.findall(f".//{k.lower()}")[0].text = str(v).upper()
        elif k.lower() in [
            "aerosol_type",
            "mid_latitude",
            "ozone_content",
            "cirrus_correction",
        ]:
            # e.g. "aerosol_type" ---> "Aerosol_Type"
            tag_name = "_".join([s.capitalize() for s in k.lower().split("_")])
            etree.findall(f".//{tag_name}")[0].text = str(v).upper()
        elif k.lower() == "dem_terrain_correction" and v:
            etree.findall(".//DEM_Directory")[0].text = srtm_path
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
    sample_config_path = os.path.join(__package__, "resources", SEN2COR_CONFILE_NAME)
    et = ElementTree.parse(sample_config_path)
    et = set_sen2cor_options(et, options, srtm_path)
    # Write back to file
    output_confile = os.path.join(processing_dir, SEN2COR_CONFILE_NAME)
    et.write(output_confile)
    return output_confile


def check_input_consistency(product_folder_path):
    """Check if the input product is a valid Sentinel-2 L1C product.

    :param str product_folder_path: path of the main Sentinel-2 L1C product folder
    :return:
    """
    if not os.path.isdir(product_folder_path):
        raise ValueError(
            f"{product_folder_path} is no a valid directory: specify the directory of a Sentinel-2 L1C input product"
        )
    mtd_file = glob.glob(MTD_FILENAME)
    if len(mtd_file) != 1:
        raise ValueError(
            f"{product_folder_path} is an invalid input product. Please specify the directory of a Sentinel-2 L1C input product"
        )
    img_data = glob.glob(os.path.join(product_folder_path, "*", "*", "*", "*.jp2"))
    if len(img_data) != 14:
        raise ValueError(
            f"the input product {product_folder_path} is not a valid Sentinel-2 L1C product"
        )


def find_option_definition(option_name):
    """Find in the workflow description the option definition by its name.

    :param str option_name: the name of the input option
    :return dict:
    """
    for option_def in sen2cor_l1c_l2a["WorkflowOptions"]:
        if option_def["Name"] == option_name:
            return option_def
    return None


def check_ozone_content(options, valid_ozone_values):
    """Check the validity of the ozone content option values.

    :param dict options: dictionary of the asked user options
    :param list_or_tuple valid_ozone_values: valid ozone content values as in the workflow description
    :return:
    """
    ozone_winter_values = (0, 250, 290, 330, 377, 420, 460)
    ozone_summer_values = (0, 250, 290, 331, 370, 410, 450)
    ozone_content_value = options["ozone_content"]
    mid_latitude_value = options["mid_latitude"]
    if mid_latitude_value == "auto" and (ozone_content_value not in valid_ozone_values):
        raise ValueError()
    elif mid_latitude_value == "winter" and (
        ozone_content_value not in ozone_winter_values
    ):
        raise ValueError()
    elif mid_latitude_value == "summer" and (
        ozone_content_value not in ozone_summer_values
    ):
        raise ValueError()
    return True


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
    return True


def check_nrow_ncol(roi_options):
    """Check the validity of the ROI options ``nrow_win`` and ``ncol_win``.

    :param dict roi_options: the ROI options ``row0``, ``col0``, ``nrow_win`` and ``ncol_win``
    :return bool:
    """
    for oname in ["nrow_win", "ncol_win"]:
        if roi_options[oname] % 6 != 0:
            raise ValueError("nrow_win and ncol_win must be integer divisible by 6")
    return True


def check_roi_options(roi_options):
    """Check the validity of the ROI options.

    :param dict roi_options: the ROI options ``row0``, ``col0``, ``nrow_win`` and ``ncol_win``
    :return bool:
    """
    if not roi_options:
        return True
    # if ROI options are present, they must be 4, not just a few
    if len(roi_options) != len(ROI_OPTIONS_NAMES):
        missing_options = ROI_OPTIONS_NAMES.difference(set(roi_options.keys()))
        raise ValueError(f"some ROI options are missing: {missing_options}")
    check_row_col_type(roi_options)
    check_nrow_ncol(roi_options)
    return True


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
    valid_names = [option["Name"] for option in sen2cor_l1c_l2a["WorkflowOptions"]]
    for oname, ovalue in other_options.items():
        if oname == "ozone_content":
            check_ozone_content(
                options, find_option_definition("ozone_content")["Values"]
            )
        elif oname in valid_names:
            valid_values = find_option_definition(oname)["Values"]
            if ovalue not in valid_values:
                raise ValueError(
                    f"invalid value '{ovalue}'' for '{oname}': valid values are {valid_values}"
                )
        else:
            raise ValueError(f"invalid option {oname}: valid options are {valid_names}")
    return True


def run_processing(
    product_folder_path,
    processing_dir,
    output_dir,
    options,
    sen2cor_path,
    srtm_path,
    logger=None,
):
    """Execute the processing by means of Sen2Cor tool to convert, according to the input user
    option, an input Sentinel-2 L1C product into a Sentinel-2 L2A product. The function returns
    the path of the output product.

    :param str product_folder_path: path of the main Sentinel-2 L1C product folder
    :param str processing_dir: path of the processing directory
    :param str output_dir: the output directory
    :param dict options: the user's options dictionary
    :param str sen2cor_path: path of the Sen2Cor ``L2A_Process`` script
    :param str srtm_path: path of the folder in which the SRTM DEM will be downloaded or searched
    :param logging.Logger logger: the processing logger object
    :return str:
    """
    check_input_consistency(product_folder_path)
    check_options(options)
    sen2cor_confile = create_sen2cor_confile(processing_dir, srtm_path, options)
    cmd = f"{sen2cor_path} {product_folder_path} --output_dir {output_dir} --GIP_L2A {sen2cor_confile}"
    if "resolution" in options:
        cmd += f" --resolution {options['resolution']}"
    # > /dev/null 2>&1
    stdout = open(os.devnull, "a")
    stderr = open(os.devnull, "a")
    if logger:
        logger.info("Command line and its output ...\n\n" + cmd + "\n")
        for hdlr in logger.handlers:
            if isinstance(hdlr, logging.FileHandler):
                # >> logfile_path 2>&1
                outfile = open(hdlr.baseFilename, "a")
                stdout = outfile
                stderr = outfile
    exit_status = subprocess.call(cmd, shell=True, stdout=stdout, stderr=stderr)
    if exit_status != 0:
        raise RuntimeError("Sen2Cor processing failed")
    output_path = os.path.join(output_dir, os.listdir(output_dir))
    return output_path
