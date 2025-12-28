import os
import typing as T

import pydantic
import yaml


class ConfigurationError(Exception):
    pass


class Configuration(pydantic.BaseModel):
    keeping_period: int = 14400
    excluded_workflows: T.List[str] = []
    enable_traceability: bool = False
    enable_authorization_check: bool = True
    enable_quota_check: bool = True
    default_role: T.TypedDict("Role", quota=int, profile=str) = {
        "quota": 1,
        "profile": "user",
    }
    roles: T.Dict[str, T.TypedDict("Role", quota=int, profile=str)] = {}
    untraced_workflows: T.List[str] = []
    enable_monitoring: bool = True
    monitoring_polling_time_s: int = 10


def read_esa_tf_config():
    """
    :return dict: returns esa_tf_config dictionary
    """
    esa_tf_config_file = os.getenv("ESA_TF_CONFIG_FILE", "./esa_tf.config")
    if not os.path.isfile(esa_tf_config_file):
        raise FileNotFoundError(
            f"{esa_tf_config_file!r} not found, please define the correct path "
            f"using the environment variable ESA_TF_CONFIG_FILE"
        )
    with open(esa_tf_config_file) as file:
        esa_tf_config = yaml.load(file, Loader=yaml.FullLoader)
    try:
        esa_tf_configuration_object = Configuration(**esa_tf_config)
    except ValueError as exc:
        raise ConfigurationError(
            f"invalid configuration file esa_tf.config: {exc!r}"
        ) from exc
    return esa_tf_configuration_object.dict()
