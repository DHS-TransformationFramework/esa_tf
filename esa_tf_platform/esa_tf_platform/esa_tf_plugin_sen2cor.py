import glob
import logging
import os
import shutil
import subprocess
from xml.etree import ElementTree


SEN2COR_CONFILE_NAME = "L2A_GIPP.xml"
SRTM_DOWNLOAD_ADDRESS = (
    "http://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/"
)
MTD_FILENAME = "MTD_MSIL1C.xml"
ROI_OPTIONS_NAMES = {"row0", "col0", "nrow_win", "ncol_win"}


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


def compress_and_move(src_dir, dst_dir, format="zip"):
    """Create in the output folder the compressed output file starting from the Sen2Cor output
    Sentinel-2 L2A folder. The function returns the path of the compressed output file.

    :param str src_dir: the folder path in which the Sen2Cor output is saved
    :param str dst_dir: the output directory
    :param str format: the archive format, default value: ``zip``
    :return str:
    """
    sen2cor_filename = os.listdir(src_dir)[0]
    # remove the ".SAFE" string (if present) from the output Sen2Cor folder
    output_filename = os.path.splitext(sen2cor_filename)[0]
    src_path = os.path.join(src_dir, sen2cor_filename)
    dst_path = os.path.join(dst_dir, output_filename)
    # the file extension is automatically added by the "shutil.make_archive" function
    shutil.make_archive(dst_path, format, src_path)
    return dst_path


def run_processing(
    product_file,
    *,
    workflow_options,
    processing_dir,
    output_dir,
    sen2cor_script_file=None,
    srtm_dir=None,
    logger=None,
):
    """Execute the processing by means of Sen2Cor tool to convert, according to the input user
    option, an input Sentinel-2 L1C product into a Sentinel-2 L2A product. The function returns
    the path of the output product.

    :param str product_file: path of the main Sentinel-2 L1C product folder
    :param dict workflow_options: the user's options dictionary
    :param str processing_dir: path of the processing directory
    :param str output_dir: the output directory
    :param str sen2cor_script_file: path of the Sen2Cor ``L2A_Process`` script. If not defined the
    environment variable ``SEN2COR_SCRIPT_FILE`` will be used.
    :param str srtm_dir: path of the folder in which the SRTM DEM will be downloaded or searched. If not defined the
    environment variable ``SRTM_DIR`` will be used. In case this variable does not exist a directory dem
    in the ``processing_dir`` will be created.
    :param logging.Logger logger: the processing logger object
    :return str:
    """
    if sen2cor_script_file is None:
        sen2cor_script_file = os.getenv("SEN2COR_SCRIPT_FILE", "Sen2Cor-02.09.00-Linux64/bin/L2A_Process")
    if srtm_dir is None:
        srtm_dir = os.getenv("SRTM_DIR", None)

    check_input_consistency(product_file)
    check_options(workflow_options)
    # if the "srtm_path" is not defined, the SRTM tile is downloaded inside a dedicate folder
    # into the processing-dir
    if not srtm_dir:
        srtm_dir = os.path.join(processing_dir, "dem")
        os.makedirs(srtm_dir, exist_ok=True)
    if srtm_dir and not os.path.isdir(srtm_dir):
        raise ValueError(
            f"{srtm_dir} not not found, please define it using the environment variable 'SRTM_DIR'"
        )
    # creation of the folder in which the Sen2Cor output will be created before compressing and
    # moving it into the output directory
    output_binder_dir = os.path.join(processing_dir, "output_binder_dir")
    os.makedirs(output_binder_dir, exist_ok=True)
    # creation of the Sen2Cor configuration files inside the processing-dir
    sen2cor_confile = create_sen2cor_confile(workflow_options, srtm_dir, workflow_options)
    # running the Sen2Cor script
    cmd = f"{sen2cor_script_file} {product_file} --output_dir {output_binder_dir} --GIP_L2A {sen2cor_confile}"
    if "resolution" in workflow_options:
        cmd += f" --resolution {workflow_options['resolution']}"
    exit_status = subprocess.call(cmd, shell=True)
    if exit_status != 0:
        raise RuntimeError("Sen2Cor processing failed")
    # creation of the output archive file
    output_path = compress_and_move(output_binder_dir, output_dir)
    return output_path


sen2cor_l1c_l2a = {
    "Description": "Product processing from Sentinel-2 L1C to L2A. Processor V2.3.6",
    "Execute": run_processing,
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
