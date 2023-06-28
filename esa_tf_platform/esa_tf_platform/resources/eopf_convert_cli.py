import ast
import logging
import os
import sys

from eopf.product.store import (
    EOCogStore,
    EONetCDFStore,
    EOSafeStore,
    EOZarrStore,
    convert,
)

logger = logging.getLogger("eopf_safe_to_zarr")


def eopf_convert(
    input_path: str, output_path: str, target_store: str, zarr_store_options: str
):
    if os.path.splitext(input_path)[-1] == "nc":
        in_store = EONetCDFStore(input_path)
    else:
        in_store = EOSafeStore(input_path)

    if target_store == "zarr":
        target_store = EOZarrStore(output_path)
    elif target_store == "netcdf":
        target_store = EONetCDFStore(output_path)
    elif target_store == "cog":
        target_store = EOCogStore(output_path)
    else:
        raise ValueError(
            f"target_store {target_store} not recognized. "
            f"target_store shall be one of the following zarr, netcdf, cod:"
        )

    convert(in_store, target_store, target_kwargs=zarr_store_options)


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    target_store = sys.argv[3]
    zarr_store_options = ast.literal_eval(sys.argv[4])
    eopf_convert(
        input_path, output_path, target_store, zarr_store_options=zarr_store_options
    )
