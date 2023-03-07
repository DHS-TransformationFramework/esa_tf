import hashlib
import logging
import os
import time
import urllib

import cachetools
import requests
import sentinelsat
import yaml
from authlib.integrations.requests_client import OAuth2Session

logger = logging.getLogger(__name__)

SESSION_LIST = {}


class CscApi:
    def __init__(self, **hub_credentials):
        self.password = hub_credentials["password"]
        self.user = hub_credentials["user"]

        version = hub_credentials.get("version", "v1")
        self.api_url = urllib.parse.urljoin(
            hub_credentials["api_url"], f"odata/{version}/"
        )

        self.client_id = hub_credentials.get("client_id", None)
        self.token_endpoint = hub_credentials.get("token_endpoint", None)
        if self.client_id and self.token_endpoint:
            logger.info(f"using oauth2 authentication for {hub_credentials['api_url']}")
            self.authentication = "oauth2"
            self.session = OAuth2Session(
                client_id=self.client_id, token_endpoint=self.token_endpoint
            )

            self.token = self.session.fetch_token(
                self.token_endpoint,
                username=self.user,
                password=self.password,
            )
            logger.info(f"updated token")
        else:
            logger.info(f"using basic authentication for {hub_credentials['api_url']}")
            self.authentication = "basic"
            self.session = requests.session()
            self.session.auth = (self.user, self.password)

    def _ensure_token(self):
        if self.authentication is not "oauth2":
            return
        if (self.token["expires_at"] - time.time() - 60) < 0:
            self.token = self.session.fetch_token(
                token_url="https://your-token-endpoint.com/oauth/token",
                username=self.user,
                password=self.password,
            )

    def _get_product_info(self, product):
        product = os.path.splitext(product)[0]
        query_url = urllib.parse.urljoin(
           self.api_url, f"Products?$filter=startswith(Name,'{product}')"
        )
        logger.info(f"QUERY {query_url}")
        logger.info(f"PRODUCT {product}")

        response = requests.get(query_url)
        response.raise_for_status()
        product_info = response.json()["value"]
        if len(product_info) == 0:
            raise ValueError(f"{product} not found in: {self.api_url}")
        logger.info(f"PRODUCT INFO {product_info}")
        return product_info[0]

    def download(self, product, directory_path, chunk_size=8192, checksum=True):
        product_info = self._get_product_info(product)
        product_id = product_info["Id"]
        download_url = urllib.parse.urljoin(
            self.api_url, f"Products({product_id})/$value"
        )
        product_checksum = product_info.get("Checksum", None)
        if not isinstance(product_checksum, str):
            product_checksum = None
        product_basename = os.path.splitext(product)[0]
        product_path = os.path.join(directory_path, f"{product_basename}.zip")
        if checksum and not product_checksum:
            logging.warning(
                f"checksum cannot be verified, checksum not available in {self.api_url} product info"
            )
            checksum = False

        if checksum:
            hash_md5 = hashlib.md5()

        self._ensure_token()
        with self.session.get(download_url, stream=True) as response:
            response.raise_for_status()
            with open(product_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if checksum:
                        hash_md5.update(chunk)
                    f.write(chunk)
        if checksum:
            if not (hash_md5.hexdigest() == product_checksum):
                raise RuntimeError(
                    f"checksum does not match, failed to download product: {product}"
                )

        return product_path


class DhusApi:
    def __init__(self, **hub_credentials):
        self.password = hub_credentials["password"]
        self.user = hub_credentials["user"]
        self.api_url = hub_credentials["api_url"]

        self.api = sentinelsat.SentinelAPI(
            api_url=self.api_url,
            user=self.user,
            password=self.password,
        )

    def _get_product_id(self, product):
        identifier = os.path.splitext(product)[0]
        uuid_products = self.api.query(identifier=identifier)
        if len(uuid_products) == 0:
            raise ValueError(f"{product} not found in: {self.api_url}")
        return list(uuid_products)[0]

    def download(self, product, directory_path, checksum=True):
        uuid_product = self._get_product_id(product)
        product_info = self.api.download(
            uuid_product,
            directory_path=directory_path,
            checksum=checksum,
            nodefilter=None,
        )
        return product_info["path"]


def read_hub_config(
    hubs_credential_file,
):
    """
    Read credentials from the hubs_credential_file.
    """
    with open(hubs_credential_file) as file:
        hubs_credentials = yaml.load(file, Loader=yaml.FullLoader)
    return hubs_credentials


def chachekey(hubs_credentials_file):
    modification_time = os.stat(hubs_credentials_file).st_mtime
    return cachetools.keys.hashkey(hubs_credentials_file, modification_time)


@cachetools.cached(
    cache=cachetools.TTLCache(maxsize=1, ttl=10),
    key=chachekey,
    info=True,
)
def update_api_list(hubs_config_file):
    hubs_config = read_hub_config(hubs_config_file)

    global SESSION_LIST
    for hub in SESSION_LIST.keys() - hubs_config.keys():
        SESSION_LIST.pop(hub, None)

    apis = {"dhus-api": DhusApi, "csc-api": CscApi}

    for hub_name, hub_config in hubs_config.items():
        api_type = hub_config.get("api_type", None)
        if api_type is None:
            logger.warning(
                f"api_type not defined for {hub_name} in {hubs_config_file} configuration file, "
                f"'hus-api' will be used."
            )
            api_type = "dhus-api"

        api = apis.get(api_type, None)
        if api is None:
            logger.warning(
                f"error in in {hubs_config_file} configuration file, "
                f"{api_type} api_type not found, it can take only the following values: {list(apis)}"
            )
        else:
            SESSION_LIST[hub_name] = api(**hub_config["credentials"])

    return SESSION_LIST


def download(
    product, *, processing_dir, hubs_config_file, hub_name=None, order_id=None
):
    """
    Download the product from the first hub in the hubs_credentials_file that publishes the product
    """

    session_list = update_api_list(hubs_config_file)

    if hub_name:
        if hub_name not in session_list:
            raise ValueError(f"{hub_name} not found")
        session_list = {hub_name: session_list[hub_name]}

    product_path = None
    for hub_name, session in session_list.items():
        logger.info(f"trying to download data from {hub_name}")
        try:
            product_path = session.download(
                product,
                directory_path=processing_dir,
            )
        except Exception:
            logger.exception(
                f"not able to download from {hub_name}, an error occurred:"
            )
        if product_path:
            break
    if product_path is None:
        raise ValueError(
            f"order_id {order_id}: could not download product from {list(session_list)}"
        )
    return product_path