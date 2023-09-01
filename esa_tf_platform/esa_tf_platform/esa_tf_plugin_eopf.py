import functools
import logging
import os
import pathlib
import subprocess

import pkg_resources

logger = logging.getLogger(__name__)

store_suffix = {"zarr": "zarr", "cog": "cog", "netcdf": "nc"}


PRODUCT_TYPE_ZARR = [
    "WV_SLC__1S",
    "IW_SLC__1S",
    "EW_SLC__1S",
    "IW_GRDH_1S",
    "EW_GRDH_1S",
    "IW_GRDM_1S",
    "EW_GRDM_1S",
    "IW_OCN__2S",
    "EW_OCN__2S",
    "S2MSI1C",
    "S2MSI2A",
    "SR_1_SRA_BS",
    "SL_1_RBT___",
    "OL_1_EFR___",
    "OL_1_ERR___",
    "OL_2_LRR___",
    "SL_2_LST___",
    "SL_2_FRP___",
    "SY_2_SYN___",
    "SR_2_LAN___",
]


PRODUCT_TYPE_COG = [
    "WV_SLC__1S",
    "IW_SLC__1S",
    "EW_SLC__1S",
    "IW_GRDH_1S",
    "EW_GRDH_1S",
    "IW_GRDM_1S",
    "EW_GRDM_1S",
    "IW_OCN__2S",
    "EW_OCN__2S",
    "S2MSI1C",
    "S2MSI2A",
    "SR_1_SRA_BS",
    "SL_1_RBT___",
    "OL_1_EFR___",
    "OL_1_ERR___",
    "OL_2_LRR___",
    "SL_2_LST___",
    "SL_2_FRP___",
    "SY_2_SYN___",
    "SR_2_LAN___",
]


PRODUCT_TYPE_NC = [
    "IW_OCN__2S",
    "EW_OCN__2S",
    "S2MSI1C",
    "S2MSI2A",
    "SR_1_SRA_BS",
    "SL_1_RBT___",
    "OL_1_EFR___",
    "OL_1_ERR___",
    "OL_2_LRR___",
    "SL_2_LST___",
    "SL_2_FRP___",
    "SY_2_SYN___",
    "SR_2_LAN___",
]


def run_processing(
    product_path, *, workflow_options, processing_dir, output_dir, target_store="zarr"
):
    stem = pathlib.Path(product_path).stem
    suffix = store_suffix[target_store]
    output_product = os.path.join(output_dir, f"{stem}.{suffix}")
    eopf_convert_cli = pkg_resources.resource_filename(
        __package__, os.path.join("resources", "eopf_convert_cli.py")
    )
    cmd = (
        f"conda run -n eopf python "
        f"{eopf_convert_cli} "
        f"{product_path} "
        f"{output_product} "
        f"{target_store} "
        f'"{workflow_options.__repr__()}"'
    )

    logger.info(f"Executing command: {cmd}")

    process = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True
    )
    while process.poll() is None:
        lines = iter(process.stdout.readline, b"")
        try:
            line = next(lines)
        except:
            continue
        logger.info(line)

    process.stdout.close()
    exit_status = process.returncode
    if exit_status != 0:
        raise subprocess.CalledProcessError(exit_status, cmd)

    return output_product


convert_to_zarr_run_processing = functools.partial(run_processing, target_store="zarr")
convert_to_netcdf_run_processing = functools.partial(
    run_processing, target_store="netcdf"
)
convert_to_cog_run_processing = functools.partial(run_processing, target_store="cog")


eopf_to_zarr_workflow_api = {
    "WorkflowName": "eopf_convert_to_zarr",
    "WorkflowOptions": {
        "dask_compression": {
            "Description": "Type of compression",
            "Type": "string",
            "Default": "zstd",
            "Enum": ["zstd", "blosclz", "lz4", "lz4hc", "zlib", "snappy"],
        },
        "dask_comp_level": {
            "Description": "Compression level",
            "Type": "integer",
            "Default": 1,
            "Enum": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        },
        "dask_shuffle": {
            "Description": "Shuffle: NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1)",
            "Type": "integer",
            "Default": 2,
            "Enum": [0, 1, 2, -1],
        },
    },
    "Description": "EOPF plugin for converting Sentinel-1, Sentinel-2 and "
    "Sentinel-3 SAFE in zarr format",
    "Execute": "esa_tf_platform.esa_tf_plugin_eopf.convert_to_zarr_run_processing",
    "InputProductType": PRODUCT_TYPE_ZARR,
    "OutputProductType": None,
    "WorkflowVersion": "0.1",
    "ProcessorName": "eopf",
    "ProcessorVersion": "1.2.2",
    "SupportTraceabilty": True,
}


eopf_to_netcdf_workflow_api = {
    "WorkflowName": "eopf_convert_to_netcdf",
    "WorkflowOptions": {
        "netcdf_compression": {
            "Description": "Activate the compression",
            "Type": "boolean",
            "Default": True,
            "Enum": [True, False],
        },
        "netcdf_comp_level": {
            "Description": "Compression level",
            "Type": "integer",
            "Default": 1,
            "Enum": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        },
        "netcdf_shuffle": {
            "Description": "Activate the shuffle",
            "Type": "string",
            "Default": "YES",
            "Enum": ["YES", "NO"],
        },
    },
    "Description": "EOPF plugin for converting Sentinel-1, Sentinel-2 and "
    "Sentinel-3 SAFE in netcdf format",
    "Execute": "esa_tf_platform.esa_tf_plugin_eopf.convert_to_netcdf_run_processing",
    "InputProductType": PRODUCT_TYPE_NC,
    "OutputProductType": None,
    "WorkflowVersion": "0.1",
    "ProcessorName": "eopf",
    "ProcessorVersion": "1.2.2",
    "SupportTraceabilty": True,
}


eopf_to_cog_workflow_api = {
    "WorkflowName": "eopf_convert_to_cog",
    "WorkflowOptions": {
        "cog_compression": {
            "Description": "Type of compression",
            "Type": "string",
            "Default": "DEFLATE",
            "Enum": [
                "NONE",
                "LZW",
                "JPEG",
                "DEFLATE",
                "ZSTD",
                "WEBP",
                "LERC",
                "LERC_DEFLATE",
                "LERC_ZSTD",
                "LZMA",
            ],
        },
    },
    "Description": "EOPF plugin for converting Sentinel-1, Sentinel-2 and "
    "Sentinel-3 SAFE in COG format",
    "Execute": "esa_tf_platform.esa_tf_plugin_eopf.convert_to_cog_run_processing",
    "InputProductType": PRODUCT_TYPE_COG,
    "OutputProductType": None,
    "WorkflowVersion": "0.1",
    "ProcessorName": "eopf",
    "ProcessorVersion": "1.2.2",
    "SupportTraceabilty": True,
}
