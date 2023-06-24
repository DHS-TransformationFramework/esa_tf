import logging
import os
import pathlib
import subprocess

import pkg_resources

logger = logging.getLogger(__name__)


def run_processing(
    product_path,
    *,
    workflow_options,
    processing_dir,
    output_dir
):
    stem = pathlib.Path(product_path).stem
    output_product = os.path.join(output_dir, f"{stem}.zarr")
    eopf_safe_to_zarr_cli = pkg_resources.resource_filename(
        __package__, os.path.join("resources", "eopf_safe_to_zarr_cli.py")
    )
    cmd = f"conda run -n eopf python {eopf_safe_to_zarr_cli} {product_path} {output_product}"

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


eopf_safe_to_zarr_workflow_api = {
    "WorkflowName": "eopf_safe_to_zarr",
    "WorkflowOptions": {
        "dask_compression": {
            "Description": "",
            "Type": "string",
            "Default": "ZSTD",
            "Enum": ["ZSTD", "BLOSCLZ", "LZ4", "LZ4HC", "ZLIB", "SNAPPY"]
        },
        "dask_comp_level": {
            "Description": "",
            "Type": "int",
            "Default": 1,
            "Enum": [1, 2, 3, 4, 5, 6, 7, 8, 9]
        },
        "dask_shuffle": {
            "Description": "shuffle: NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1)",
            "Type": "int",
            "Default": 2,
            "Enum": [0, 1, 2, -1]
        }

    },
    "Description": "",
    "Execute": "esa_tf_platform.esa_tf_plugin_eopf.run_processing",
    "InputProductType": "S2MSI1C",
    "OutputProductType": None,
    "WorkflowVersion": "0.1",
    "WorkflowOptions": {},
    "ProcessorName": "eopf",
    "ProcessorVersion": "1.2.2",
    "SupportTraceabilty": True,
}
