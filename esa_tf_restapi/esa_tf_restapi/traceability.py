import pydantic
import subprocess
import os
import yaml


TRACEABILITY_CONFIG_FILENAME = "traceability_config.yaml"
TRACETOOL = "tracetool-1.2.4.jar"
KEY_FILENAME = "secret.txt"


class ConfigurationError(Exception):
    pass


class Configuration(pydantic.BaseModel):

    service_url: pydantic.HttpUrl
    username: pydantic.SecretStr
    password: pydantic.SecretStr
    key_fingerprint: pydantic.SecretStr
    passphrase: pydantic.SecretStr
    service_context: str
    service_type: str
    service_provider: str
    event_type: str = "CREATE"


def read_traseability_config():
    """Read and return the traceability credentials configuration file.

    :return Configuration:
    """
    traceability_config_file = os.path.join(os.getenv("CONFIG_DIR"), TRACEABILITY_CONFIG_FILENAME)
    if not os.path.isfile(traceability_config_file):
        raise FileNotFoundError(f"{traceability_config_file!r} not found")
    with open(traceability_config_file) as file:
        traceability_config = yaml.load(file, Loader=yaml.FullLoader)
    try:
        trace_configuration_object = Configuration(**traceability_config)
    except ValueError as exc:
        raise ConfigurationError(
            f"invalid configuration file traceability_credentials.yaml: {exc!r}"
        ) from exc
    return trace_configuration_object


def import_key(traceability_config, key_path, tracetool_path):
    """Import a key if not already imported.

    :param Configuration traceability_config: traceability configuration
    :param str key_path: path of the traceability service secret key
    :param str tracetool_path: path of the trace tool .jar file
    :return:
    """
    out_before = subprocess.run(['gpg -k'], shell=True, capture_output=True, text=True)
    if traceability_config.key_fingerprint.get_secret_value() not in out_before.stdout:
        cmd = f"java -jar {tracetool_path} --import {key_path} {traceability_config.passphrase.get_secret_value()}"
        process = subprocess.run(cmd, shell=True)
        if process.returncode != 0:
            sanitised_cmd = f"java -jar {tracetool_path} --import {key_path} {traceability_config.passphrase}"
            raise subprocess.CalledProcessError(process.returncode, sanitised_cmd)
        out_after = subprocess.run(['gpg -k'], shell=True, capture_output=True, text=True)
        if traceability_config.key_fingerprint.get_secret_value() not in out_after.stdout:
            raise RuntimeError(f"key has not be imported: {out_after.stderr}")


class Trace(object):

    def __init__(self):
        self.traceability_config = read_traseability_config()
        self.tracetool_path = os.path.join(os.getenv("CONFIG_DIR"), TRACETOOL)
        self.key_path = os.path.join(os.getenv("CONFIG_DIR"), KEY_FILENAME)
        import_key(self.traceability_config, self.key_path, self.tracetool_path)



