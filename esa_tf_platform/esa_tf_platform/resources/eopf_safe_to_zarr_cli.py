import sys

import click
from eopf.product.store import EOSafeStore, EOZarrStore, convert


def eopf_safe_to_zarr(input_path, output_path):
    convert(EOSafeStore(input_path), EOZarrStore(output_path))


if __name__ == "__main__":
    eopf_safe_to_zarr(sys.argv[1], sys.argv[2])
