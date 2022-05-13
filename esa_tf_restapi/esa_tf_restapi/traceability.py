import json
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
    service_type: str = "Production"
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

    :param Configuration traceability_config: the traceability configuration
    :param str key_path: path of the traceability service secret key
    :param str tracetool_path: path of the trace tool .jar file
    :return:
    """
    out_before = subprocess.run(['gpg -k'], shell=True, capture_output=True, text=True)
    if traceability_config.key_fingerprint.get_secret_value() not in out_before.stdout:
        cmd = (
            f"java -jar {tracetool_path} --import {key_path} "
            f"{traceability_config.passphrase.get_secret_value()}"
        )
        process = subprocess.run(cmd, shell=True)
        if process.returncode != 0:
            sanitised_cmd = (
                f"java -jar {tracetool_path} --import {key_path} {traceability_config.passphrase}"
            )
            raise subprocess.CalledProcessError(process.returncode, sanitised_cmd)
        out_after = subprocess.run(['gpg -k'], shell=True, capture_output=True, text=True)
        if traceability_config.key_fingerprint.get_secret_value() not in out_after.stdout:
            raise RuntimeError(f"key has not be imported: {out_after.stderr}")


def initialise_trace(trace_path, traceability_config):
    """Initialise the trace, save the corresponding JSON file and return a dictionary with the same
    content.

    :param str trace_path: path of the output .json file
    :param Configuration traceability_config: the traceability configuration
    :return dict:
    """
    trace = {
        "beginningDateTime": None,
        "eventType": traceability_config.event_type,
        "platformShortName": None,
        "processorName": None,
        "processorVersion": None,
        "productType": None,
        "serviceContext": traceability_config.service_context,
        "serviceProvider": traceability_config.service_provider,
        "serviceType": traceability_config.service_type,
    }
    with open(trace_path, "w") as f:
        f.write(json.dumps(trace, indent=4, sort_keys=True))
    return trace


class Trace(object):

    def __init__(self, trace_path):
        self.traceability_config = read_traseability_config()
        self.tracetool_path = os.path.join(os.getenv("CONFIG_DIR"), TRACETOOL)
        self.key_path = os.path.join(os.getenv("CONFIG_DIR"), KEY_FILENAME)
        import_key(self.traceability_config, self.key_path, self.tracetool_path)
        self.trace_content = initialise_trace(trace_path, self.traceability_config)
        self.trace_path = trace_path

    def hash_ad_sign(self, product_path):
        cmd = (
            f"java -jar {self.tracetool_path} --hash-sign {product_path} "
            f"{self.traceability_config.key_fingerprint.get_secret_value()} "
            f"{self.traceability_config.passphrase.get_secret_value()} "
            f"{self.trace_path}"
        )
        process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if process.returncode != 0:
            sanitised_cmd = (
                f"java -jar {self.tracetool_path} --hash-sign {product_path} "
                f"{self.traceability_config.key_fingerprint} "
                f"{self.traceability_config.passphrase} "
                f"{self.trace_path}"
            )
            raise subprocess.CalledProcessError(process.returncode, sanitised_cmd)
        else:
            self.trace_content = json.loads(process.stdout)
