import json
import pydantic
import requests
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
    url_access_token: pydantic.HttpUrl
    url_push_trace: pydantic.HttpUrl
    username: pydantic.SecretStr
    password: pydantic.SecretStr
    key_fingerprint: pydantic.SecretStr
    passphrase: pydantic.SecretStr
    service_context: str
    service_type: str = "Production"
    service_provider: str
    event_type: str = "CREATE"


def read_traceability_config():
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
    """Import a TS Data Producer key, if not already imported.

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


def get_access_token(url, username, password):
    """Return a dictionary representing the access token obtained from the authentication service.

    :param str url: URL of the authentication service
    :param str username: username of the account as TS data Producer with certified key
    :param str password: password of the account as TS data Producer with certified key
    :return dict:
    """
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = f'grant_type=password&username={username}&password={password}'
    auth = ('trace-api', '')
    res = requests.post(url, headers=headers, data=data, auth=auth)
    res.raise_for_status()
    return res.json()


def push_trace(url, access_token, trace_path):
    """Push a trace on the Traceability Service.

    :param str url: URL of the service to push a trace
    :param str access_token: the access token obtained by the authentication service
    :param str trace_path: the path of the .json trace file
    :return:
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    with open(trace_path) as f:
        data = f.read().replace('\n', '')
    res = requests.post(url, headers=headers, data=data)
    res.raise_for_status()


class Trace(object):

    def __init__(self, trace_path):
        self.traceability_config = read_traceability_config()
        self.tracetool_path = os.path.join(os.getenv("CONFIG_DIR"), TRACETOOL)
        self.key_path = os.path.join(os.getenv("CONFIG_DIR"), KEY_FILENAME)
        import_key(self.traceability_config, self.key_path, self.tracetool_path)
        self.trace_content = initialise_trace(trace_path, self.traceability_config)
        self.trace_path = trace_path

    def hash_and_sign(self, product_path):
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
            with open(self.trace_path, "w") as f:
                f.write(json.dumps(self.trace_content, indent=4, sort_keys=True))

    def push(self):
        self.access_token = get_access_token(
            self.traceability_config.url_access_token,
            self.traceability_config.username.get_secret_value(),
            self.traceability_config.password.get_secret_value()
        )
        push_trace(
            self.traceability_config.url_push_trace,
            self.access_token["access_token"],
            self.trace_path
        )
