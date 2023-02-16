from unittest import mock

import pytest

from esa_tf_platform import product_download


@mock.patch(
    "sentinelsat.SentinelAPI.download",
    mock.MagicMock(return_value={"path": "product_path"}),
)
@mock.patch(
    "sentinelsat.SentinelAPI.query", mock.MagicMock(return_value={"uuid": "uuid"})
)
def test_download_product_from_hub():
    hub_credentials = {
        "api_url": "https:/apihub.copernicus.eu/apihub",
        "user": "user",
        "password": "password",
    }

    session = product_download.DhusApi(**hub_credentials)
    path = session.download(
        product="product",
        directory_path="processing_dir",
    )

    assert path == "product_path"


@mock.patch(
    "sentinelsat.SentinelAPI.download",
    mock.MagicMock(return_value={"path": "product_path"}),
)
@mock.patch("sentinelsat.SentinelAPI.query", mock.MagicMock(return_value={}))
def test_error_download_product_from_hub():

    with pytest.raises(ValueError, match=f"product not found"):
        hub_credentials = {
            "api_url": "https:/apihub.copernicus.eu/apihub",
            "user": "user",
            "password": "password",
        }

        session = product_download.DhusApi(**hub_credentials)
        session.download(
            product="product",
            directory_path="processing_dir",
        )


@mock.patch("esa_tf_platform.product_download.update_api_list")
def test_error_download_product1(update_api_list):
    api_hub = mock.MagicMock()
    api_hub.download.side_effect = [ValueError(), "product_path", "product_path"]
    update_api_list.return_value = {"hub1": api_hub, "hub2": api_hub, "hub3": api_hub}

    product_path = product_download.download(
        "product",
        processing_dir="processing_dir",
        hubs_config_file="hubs_config_file",
    )
    assert product_path == "product_path"
    assert api_hub.download.call_count == 2


@mock.patch("esa_tf_platform.product_download.update_api_list")
def test_error_download_product2(update_api_list):
    api_hub = mock.MagicMock()
    api_hub.download.side_effect = [ValueError(), ValueError(), "product_path"]
    update_api_list.return_value = {"hub1": api_hub, "hub2": api_hub, "hub3": api_hub}

    product_path = product_download.download(
        "product",
        processing_dir="processing_dir",
        hubs_config_file="hubs_config_file",
    )
    assert product_path == "product_path"
    assert api_hub.download.call_count == 3


@mock.patch("esa_tf_platform.product_download.update_api_list")
def test_error_download_product3(update_api_list):
    api_hub = mock.MagicMock()
    api_hub.download.side_effect = [ValueError(), ValueError(), ValueError()]
    update_api_list.return_value = {"hub1": api_hub, "hub2": api_hub, "hub3": api_hub}

    with pytest.raises(ValueError, match="could not download product"):
        product_path = product_download.download(
            "product",
            processing_dir="processing_dir",
            hubs_config_file="hubs_config_file",
        )
