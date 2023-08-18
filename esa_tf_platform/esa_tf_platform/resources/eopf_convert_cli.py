import ast
import logging
import sys

from eopf.product.store import (
    EOCogStore,
    EONetCDFStore,
    EOSafeStore,
    EOZarrStore,
    convert,
)

logger = logging.getLogger("eopf_convert")


def eopf_convert(
    input_path: str, output_path: str, target_store: str, target_kwargs: str
):
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
            f"target_store shall be one of the following zarr, netcdf, cog:"
        )

    logger.info(
        f"Converting {input_path} in format {target_store} using the following options: {target_kwargs}"
    )
    logger.info(f"convert({in_store}, {target_store}, target_kwargs={target_kwargs})")
    convert(in_store, target_store, target_kwargs=target_kwargs)


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    target_store = sys.argv[3]
    target_kwargs = ast.literal_eval(sys.argv[4])
    eopf_convert(input_path, output_path, target_store, target_kwargs=target_kwargs)
