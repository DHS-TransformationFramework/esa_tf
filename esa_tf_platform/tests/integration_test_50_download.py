import os

from esa_tf_platform import product_download

CREODIAS_PRODUCT = os.getenv(
    "creodias_product",
    "S2B_MSIL1C_20230213T171419_N0509_R112_T17VMH_20230213T202121.SAFE",
)
SCIHUB_PRODUCT = os.getenv(
    "scihub_product",
    "S2B_MSIL1C_20230214T132239_N0509_R124_T28WDA_20230214T151956.SAFE",
)
HUB_CONFIG_FILE = os.getenv("hub_config_file", "../esa_tf/config/hubs_credentials.yaml")

OUTPATH = "./tests/data"


def test_download_scihub():
    os.makedirs(OUTPATH, exist_ok=True)
    hub_config = product_download.read_hub_config(HUB_CONFIG_FILE)
    credentials = hub_config.get("scihub", {}).get("credentials", None)

    session = product_download.DhusApi(**credentials)
    path = session.download(
        SCIHUB_PRODUCT,
        directory_path=OUTPATH,
    )
    product_basename = os.path.splitext(SCIHUB_PRODUCT)[0]
    expected_path = os.path.join(OUTPATH, f"{product_basename}.zip")
    assert os.path.normpath(path) == os.path.normpath(expected_path)


def test_download_creodias():
    os.makedirs(OUTPATH, exist_ok=True)
    hub_config = product_download.read_hub_config(HUB_CONFIG_FILE)
    credentials = hub_config.get("creodias", {}).get("credentials", None)

    session = product_download.CscApi(**credentials)
    path = session.download(
        CREODIAS_PRODUCT,
        directory_path=OUTPATH,
    )
    product_basename = os.path.splitext(CREODIAS_PRODUCT)[0]
    expected_path = os.path.join(OUTPATH, f"{product_basename}.zip")
    assert os.path.normpath(path) == os.path.normpath(expected_path)


def test_download():
    os.makedirs(OUTPATH, exist_ok=True)

    path = product_download.download(
        CREODIAS_PRODUCT,
        processing_dir=OUTPATH,
        hubs_config_file=HUB_CONFIG_FILE,
    )

    product_basename = os.path.splitext(CREODIAS_PRODUCT)[0]
    expected_path = os.path.join(OUTPATH, f"{product_basename}.zip")
    assert os.path.normpath(path) == os.path.normpath(expected_path)
