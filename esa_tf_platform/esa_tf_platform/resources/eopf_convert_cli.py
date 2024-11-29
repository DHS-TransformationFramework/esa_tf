import ast
import logging
import sys


from eopf.store.convert import convert

logger = logging.getLogger("eopf_convert")


def eopf_convert(
    input_path: str, output_path: str, target_format: str, target_store_kwargs: str
):
    logger.info(
        f"Converting {input_path} in format {target_format} using the following options: {target_store_kwargs}"
    )
    convert(
        input_path,
        output_path,
        target_format=target_format,
        target_store_kwargs=target_store_kwargs,
    )


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    target_format = sys.argv[3]
    target_store_kwargs = ast.literal_eval(sys.argv[4])
    eopf_convert(
        input_path, output_path, target_format, target_store_kwargs=target_store_kwargs
    )
