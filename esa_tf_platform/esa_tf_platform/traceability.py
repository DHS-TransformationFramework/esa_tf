import json
import os
import subprocess

import pydantic
import requests
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
    traceability_config_file = os.path.join(
        os.getenv("CONFIG_DIR"), TRACEABILITY_CONFIG_FILENAME
    )
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
    out_before = subprocess.run(["gpg -k"], shell=True, capture_output=True, text=True)
    if traceability_config.key_fingerprint.get_secret_value() not in out_before.stdout:
        cmd = (
            f"java -jar {tracetool_path} --import {key_path} "
            f"{traceability_config.passphrase.get_secret_value()}"
        )
        process = subprocess.run(cmd, shell=True)
        if process.returncode != 0:
            sanitised_cmd = f"java -jar {tracetool_path} --import {key_path} {traceability_config.passphrase}"
            raise subprocess.CalledProcessError(process.returncode, sanitised_cmd)
        out_after = subprocess.run(
            ["gpg -k"], shell=True, capture_output=True, text=True
        )
        if (
            traceability_config.key_fingerprint.get_secret_value()
            not in out_after.stdout
        ):
            raise RuntimeError(f"key has not be imported: {out_after.stderr}")


def initialise_trace(traceability_config):
    """Return a dictionary with the trace content.

    :param Configuration traceability_config: the traceability configuration
    :return dict:
    """
    trace = {
        "beginningDateTime": "",
        "eventType": traceability_config.event_type,
        "platformShortName": "",
        "processorName": "",
        "processorVersion": "",
        "productType": "",
        "serviceContext": traceability_config.service_context,
        "serviceProvider": traceability_config.service_provider,
        "serviceType": traceability_config.service_type,
    }
    return trace


def get_access_token(url, username, password):
    """Return a dictionary representing the access token obtained from the authentication service.

    :param str url: URL of the authentication service
    :param str username: username of the account as TS data Producer with certified key
    :param str password: password of the account as TS data Producer with certified key
    :return dict:
    """
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = f"grant_type=password&username={username}&password={password}"
    auth = ("trace-api", "")
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
        data = f.read().replace("\n", "")
    res = requests.post(url, headers=headers, data=data)
    res.raise_for_status()
    return res


class Trace(object):
    """
    Create a trace instance. This is typical workflow to generate, sign and push a trace:

    - initialising the trace specifying the path of the output .json trace file
    - adding to the trace hash and hashList attributes specifying the input product
    - updating the trace attributes with info about the input product and the processor
    - signing the trace using the TS Data Producer key
    - pushing the trace to the Traceability service
    """

    def __init__(self, trace_path):
        """
        :param str trace_path:
        """
        self.traceability_config = read_traceability_config()
        self.tracetool_path = os.path.join(os.getenv("CONFIG_DIR"), TRACETOOL)
        self.key_path = os.path.join(os.getenv("CONFIG_DIR"), KEY_FILENAME)
        import_key(self.traceability_config, self.key_path, self.tracetool_path)
        self.trace_path = trace_path
        self.trace_content = initialise_trace(self.traceability_config)
        self.save()
        self.access_token = None
        self.signed = False
        self.pushed = False

    def save(self):
        with open(self.trace_path, "w") as f:
            f.write(json.dumps(self.trace_content, indent=4, sort_keys=True))

    def hash(self, product_path):
        """
        :param str product_path: full path of the output .json trace file
        """
        cmd = f"java -jar {self.tracetool_path} --hash {product_path} {self.trace_path}"
        process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)
        else:
            self.trace_content = json.loads(process.stdout)
            self.save()

    def update_attributes(self, attributes):
        """Update the trace attributes according to the `attributes` dictionary. Attribute names
        and value types must be the same of the existing trace. If the trace has been already
        signed, the attributes can not be updated.

        :param dict attributes: attributes dictionary
        """
        if not self.signed:
            for attr, val in attributes.items():
                if attr not in self.trace_content:
                    raise ValueError(
                        f"attributes '{attr}' is not present in the trace .json file"
                    )
                if not isinstance(val, type(self.trace_content[attr])):
                    raise ValueError(
                        (
                            f"invalid type for attributes '{attr}', "
                            f"required {type(self.trace_content[attr])}, given {type(val)}"
                        )
                    )
                self.trace_content[attr] = val
            self.save()
        else:
            raise RuntimeError(f"trace is already signed, it can not be modified")

    def sign(self):
        cmd = (
            f"java -jar {self.tracetool_path} --sign "
            f"{self.traceability_config.key_fingerprint.get_secret_value()} "
            f"{self.traceability_config.passphrase.get_secret_value()} "
            f"{self.trace_path}"
        )
        process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if process.returncode != 0:
            sanitised_cmd = (
                f"java -jar {self.tracetool_path} --sign "
                f"{self.traceability_config.key_fingerprint} "
                f"{self.traceability_config.passphrase} "
                f"{self.trace_path}"
            )
            raise subprocess.CalledProcessError(process.returncode, sanitised_cmd)
        else:
            self.trace_content = json.loads(process.stdout)
            self.save()
            self.signed = True

    def push(self):
        self.access_token = get_access_token(
            self.traceability_config.url_access_token,
            self.traceability_config.username.get_secret_value(),
            self.traceability_config.password.get_secret_value(),
        )
        res = push_trace(
            self.traceability_config.url_push_trace,
            self.access_token["access_token"],
            self.trace_path,
        )
        self.trace_content = res.json()
        self.pushed = True
