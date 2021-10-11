import os
from xml.etree import ElementTree


SEN2COR_CONFILE_NAME = "L2A_GIPP.xml"
SRTM_DOWNLOAD_ADDRESS = (
    "http://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/"
)

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
            "Description": "Target resolution, can be 10, 20 or 60m. If omitted, only 20 and 10m resolutions will be processed",
            "Type": "boolean",
            "Default": True,
            "Values": [10, 20, 60],
        },
    ],
}


def set_sen2cor_options(etree, options, srtm_folder):
    for k, v in options.items():
        if k.lower() in ["row0", "col0", "nrow_win", "ncol_win"]:
            etree.findall(f".//{k}")[0].text = str(v).upper()
        elif k.lower() in [
            "aerosol_type",
            "mid_latitude",
            "ozone_content",
            "cirrus_correction",
        ]:
            tag_name = "_".join([s.capitalize() for s in k.split("_")])
            etree.findall(f".//{tag_name}")[0].text = str(v).upper()
        elif k.lower() == "dem_terrain_correction" and v:
            etree.findall(".//DEM_Directory")[0].text = srtm_folder
            etree.findall(".//DEM_Reference")[0].text = SRTM_DOWNLOAD_ADDRESS
    return etree


def create_sen2cor_confile(processing_dir, srtm_folder, options):
    # Read the default Sen2Cor configuration file
    sample_config_path = os.path.join(__package__, "resources", SEN2COR_CONFILE_NAME)
    et = ElementTree.parse(sample_config_path)
    et = set_sen2cor_options(et, options, srtm_folder)
    # Write back to file
    output_confile = os.path.join(processing_dir, SEN2COR_CONFILE_NAME)
    et.write(output_confile)
    return et
