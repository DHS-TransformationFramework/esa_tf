import logging
import os
import zipfile
from unittest import mock

import pkg_resources
import pytest

from esa_tf_platform import workflows

dummy_workflow_config1 = {"conf": "1"}
dummy_workflow_config2a = {"conf": "2a"}
dummy_workflow_config2b = {"conf": "2b"}
dummy_workflow_config3a = {"conf": "3a"}
dummy_workflow_config3b = {"conf": "3b"}

WORKFLOWS1 = {
    "wokflow1": {
        "WorkflowName": "Name",
        "Execute": "Execute",
        "Description": "Description",
        "InputProductType": "S2MSI1C",
        "OutputProductType": "OutputProductType",
        "WorkflowVersion": "1.0",
        "WorkflowOptions": {},
    },
    "wokflow2": {
        "WorkflowName": "Name",
        "Execute": "Execute",
        "Description": "Description",
        "InputProductType": "S2MSI1C",
        "OutputProductType": "OutputProductType",
        "WorkflowVersion": "1.0",
        "WorkflowOptions": {},
    },
}

WORKFLOWS2 = {
    "wokflow1": {
        "WorkflowName": "Name",
        "Execute": "Execute",
        "Description": "Description",
        "InputProductType": "S2MSI1C",
        "OutputProductType": "OutputProductType",
        "WorkflowVersion": "1.0",
        "WorkflowOptions": {},
    },
    "wokflow2": {
        "WorkflowName": "Name",
        "Description": "Description",
    },
}


WORKFLOWS3 = {
    "wokflow1": {
        "WorkflowName": "Name",
        "Execute": "Execute",
        "Description": "Description",
        "InputProductType": "InputProductType",
        "OutputProductType": "OutputProductType",
        "WorkflowVersion": "1.0",
        "WorkflowOptions": {},
    },
}


@pytest.fixture
def dummy_duplicated_entrypoints():
    specs = [
        "workflow1 = esa_tf_platform.tests.test_workflows:dummy_workflow_config1",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2a",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2b",
        "workflow3 = esa_tf_platform.tests.test_workflows:dummy_workflow_config3a",
        "workflow3 = esa_tf_platform.tests.test_workflows:dummy_workflow_config3b",
    ]
    eps = [pkg_resources.EntryPoint.parse(spec) for spec in specs]
    return eps


@pytest.mark.filterwarnings("ignore:Found")
def test_remove_duplicates(dummy_duplicated_entrypoints, caplog):
    with caplog.at_level(logging.INFO):
        entrypoints = workflows.remove_duplicates(dummy_duplicated_entrypoints)
    assert "found 2 entrypoints" in caplog.text
    assert len(entrypoints) == 3


def test_remove_duplicates_warnings(dummy_duplicated_entrypoints, caplog):

    with caplog.at_level(logging.INFO):
        _ = workflows.remove_duplicates(dummy_duplicated_entrypoints)

    warnings = caplog.text.split("WARNING")
    warnings = [warn for warn in warnings if warn]
    assert len(warnings) == 2
    assert "found 2 entrypoints" in warnings[0]
    assert "found 2 entrypoints" in warnings[1]


@mock.patch("pkg_resources.EntryPoint.load", mock.MagicMock(return_value=None))
def test_workflows_dict_from_pkg():
    specs = [
        "workflow1 = esa_tf_platform.tests.test_workflows:dummy_workflow_config1",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2a",
    ]
    entrypoints = [pkg_resources.EntryPoint.parse(spec) for spec in specs]
    wk = workflows.workflow_dict_from_pkg(entrypoints)
    assert len(wk) == 2
    assert wk.keys() == set(("workflow1", "workflow2"))


@mock.patch("pkg_resources.EntryPoint.load", mock.MagicMock(return_value={}))
def test_load_workflows_configurations(caplog):
    specs = [
        "workflow1 = esa_tf_platform.tests.test_workflows:dummy_workflow_config1",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2a",
        "workflow2 = esa_tf_platform.tests.test_workflows:dummy_workflow_config2b",
    ]
    with caplog.at_level(logging.INFO):
        entrypoints = [pkg_resources.EntryPoint.parse(spec) for spec in specs]
        wk = workflows.load_workflows_configurations(entrypoints)
    assert len(wk) == 2


def test_zip_product(tmpdir):
    output_folder_name = (
        "S2A_MSIL2A_20211117T093251_N9999_R136_T33NTF_20211124T093440.SAFE"
    )
    output = tmpdir.join(output_folder_name).strpath
    os.mkdir(output)
    zip_path = workflows.zip_product(output, tmpdir.strpath)

    assert os.path.isfile(zip_path) is True

    with zipfile.ZipFile(zip_path, "r") as product_zip:
        product_folder = product_zip.infolist()[0].filename

    assert product_folder.rstrip("/") == output_folder_name


@mock.patch(
    "sentinelsat.SentinelAPI.download",
    mock.MagicMock(return_value={"path": "product_path"}),
)
@mock.patch(
    "sentinelsat.SentinelAPI.query", mock.MagicMock(return_value={"uuid": "uuid"})
)
def test_download_product_from_hub():

    path = workflows.download_product_from_hub(
        product="product",
        processing_dir="processing_dir",
        hub_credentials={
            "api_url": "https:/apihub.copernicus.eu/apihub",
            "user": "user",
            "password": "password",
        },
    )
    assert path == "product_path"


@mock.patch(
    "sentinelsat.SentinelAPI.download",
    mock.MagicMock(return_value={"path": "product_path"}),
)
@mock.patch("sentinelsat.SentinelAPI.query", mock.MagicMock(return_value={}))
def test_error_download_product_from_hub():

    with pytest.raises(ValueError, match=f"product not found"):
        workflows.download_product_from_hub(
            product="product",
            processing_dir="processing_dir",
            hub_credentials={
                "api_url": "https:/apihub.copernicus.eu/apihub",
                "user": "user",
                "password": "password",
            },
        )


@mock.patch(
    "esa_tf_platform.workflows.read_hub_credentials",
    mock.MagicMock(side_effect=[{"hub1": {}, "hub2": {}, "hub3": {}}]),
)
@mock.patch(
    "esa_tf_platform.workflows.download_product_from_hub",
    mock.MagicMock(side_effect=[ValueError(), ValueError(), "product_path"]),
)
def test_error_download_product():

    product_path = workflows.download_product(
        "product",
        processing_dir="processing_dir",
        hubs_credentials_file="hubs_credentials_file",
    )
    assert product_path == "product_path"


@mock.patch(
    "esa_tf_platform.workflows.read_hub_credentials",
    mock.MagicMock(side_effect=[{"hub1": {}, "hub2": {}, "hub3": {}}]),
)
@mock.patch(
    "esa_tf_platform.workflows.download_product_from_hub",
    mock.MagicMock(side_effect=[ValueError(), "product_path", "product_path"]),
)
def test_error_download_product():

    product_path = workflows.download_product(
        "product",
        processing_dir="processing_dir",
        hubs_credentials_file="hubs_credentials_file",
    )
    assert product_path == "product_path"
    assert workflows.download_product_from_hub.call_count == 2


def test_check_workflow():
    workflow = {
        "WorkflowName": "Name",
        "Description": "Description",
        "Execute": "Execute",
        "InputProductType": "S2MSI1C",
        "OutputProductType": "OutputProductType",
        "WorkflowVersion": "1.0",
        "WorkflowOptions": {
            "Name1": {
                "Description": "Description",
                "Type": "string",
                "Default": "Default",
                "Enum": ["A", "B"],
            },
            "Name2": {
                "Description": "Description",
                "Type": "integer",
                "Default": 3,
                "Enum": [1, 2],
            },
        },
    }
    workflows.check_workflow(workflow)


def test_error_check_mandatory_workflow_keys():
    workflow = {
        "WorkflowName": "Name",
        "Description": "Description",
        "InputProductType": "S1_SLC__1S",
        "OutputProductType": "OutputProductType",
        "WorkflowVersion": "1.0",
        "WorkflowOptions": {},
    }
    with pytest.raises(ValueError, match=f"missing key"):
        workflows.check_mandatory_workflow_keys(workflow)


def test_error_check_mandatory_option_keys():
    option = {
        "Name": {
            "Default": "Default",
            "Type": "string",
            "Enum": ["A", "B"],
        }
    }
    with pytest.raises(ValueError, match=f"missing key"):
        workflows.check_mandatory_option_keys(option)


def test_error_check_valid_declared_type():
    option = {
        "Name": {
            "Description": "Description",
            "Default": "Default",
            "Type": "type",
            "Enum": ["A", "B"],
        }
    }
    with pytest.raises(ValueError, match=f"not recognized"):
        workflows.check_valid_declared_type(option)


def test_error_check_default_type():
    options = {
        "Name": {
            "Description": "Description",
            "Default": 1,
            "Type": "string",
            "Enum": ["A", "B"],
        }
    }
    with pytest.raises(ValueError, match=f"Default"):
        workflows.check_default_type(options)


def test_error_check_enum_type():
    option = {
        "Name": {
            "Description": "Description",
            "Default": "A",
            "Type": "string",
            "Enum": ["A", 1],
        }
    }
    with pytest.raises(ValueError, match=f"Enum"):
        workflows.check_enum_type(option)


@mock.patch(
    "esa_tf_platform.workflows.load_workflows_configurations",
    mock.MagicMock(side_effect=[WORKFLOWS1, WORKFLOWS2]),
)
def test_get_all_workflows():
    workflows.get_all_workflows()
    with pytest.raises(ValueError, match=f"missing key"):
        workflows.get_all_workflows()


@mock.patch(
    "esa_tf_platform.workflows.load_workflows_configurations",
    mock.MagicMock(side_effect=[WORKFLOWS1, WORKFLOWS2, WORKFLOWS3]),
)
def test_get_all_workflows(caplog):
    workflows.get_all_workflows()

    with caplog.at_level(logging.INFO):
        workflows.get_all_workflows()
    assert "missing key" in caplog.text

    with caplog.at_level(logging.INFO):
        workflows.get_all_workflows()
    assert "product type" in caplog.text
