import os
import pydantic
from typing import Any


class Settings(pydantic.BaseSettings):
    """General settings.
    """

    polling_time: int = 20
    working_dir: pydantic.DirectoryPath = "./working_dir"
    output_dir: pydantic.DirectoryPath = "/output"
    config_dir: pydantic.FilePath = "/config/esa_tf.config"
    traces_dir: pydantic.DirectoryPath = "/traces"
    hub_credentials_file: pydantic.FilePath = "/config/traceability_config.yaml"
    esa_tf_config: pydantic.FilePath = "/config/esa_tf.config"
    key_file: pydantic.FilePath = "/config/secret.txt"

    output_owner_id: str = -1
    output_group_owner_id: str = -1
    tf_debug: bool = False


settings = Settings()

