import logging
import os
import pathlib
import subprocess
import pkg_resources

logger = logging.getLogger(__name__)


def run_prcessing(product_path, output_dir, workflow_options):
    stem = pathlib.Path(product_path).stem
    output_product = os.path.join(output_dir, f"{stem}.zarr")
    eopf_safe_to_zarr_cli = pkg_resources.resource_filename(
        __package__, os.path.join("resources", "eopf_safe_to_zarr_cli.py")
    )
    cmd = f"conda run -n eopf python {eopf_safe_to_zarr_cli} --intput_path {product_path} --output_path {output_product}"
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
    "Description": "",
    "Execute": "esa_tf_platform.esa_plugin_eopf.run_processing",
    "InputProductType": "S2MSI1C",
    "OutputProductType": None,
    "WorkflowVersion": "0.1",
    "WorkflowOptions": {},
    "ProcessorName": "eopf",
    "ProcessorVersion": "1.2.2",
    "SupportTraceabilty": True,
}
