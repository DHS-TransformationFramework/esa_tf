import pytest

import esa_tf_restapi

WORKFLOW_OPTIONS = {
    "Name1": {
        "Description": "",
        "Type": "string",
        "Default": "VALUE1",
        "Enum": ["VALUE1", "VALUE2"],
    },
    "Name2": {
        "Description": "",
        "Type": "integer",
        "Default": 1,
        "Enum": [1, 2, 3, 4],
    },
    "Name3": {"Description": "", "Type": "boolean", "Default": True},
    "Name4": {"Description": "", "Type": "number"},
}


@pytest.mark.parametrize(
    "product_type,input_product_reference_name",
    [
        ("S2MSI1C", "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"),
        ("S2MSI2A", "S2B_MSIL2A_20211123T094019_N0301_R007_T18CVQ_20211123T123849.zip"),
        (
            "IW_SLC__1S",
            "S1B_IW_SLC__1SDV_20211125T040332_20211125T040401_029739_038CB1_1A18.zip",
        ),
    ],
)
def test_check_products_consistency(product_type, input_product_reference_name):

    esa_tf_restapi.api.check_products_consistency(
        product_type, input_product_reference_name, workflow_id="sen2cor_l1c_l2a"
    )


@pytest.mark.parametrize(
    "product_type,input_product_reference_name",
    [
        ("S2MSI1C", "S2B_MSIL2A_20211123T094019_N0301_R007_T18CVQ_20211123T123849.zip"),
        ("S2MSI2A", "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"),
        (
            "IW_SLC__1S",
            "S1B_EW_SLC__1SDV_20211125T040332_20211125T040401_029739_038CB1_1A18.zip",
        ),
    ],
)
def test_check_products_consistency_wrong_product(
    product_type, input_product_reference_name
):

    with pytest.raises(ValueError, match=r"input product reference"):
        esa_tf_restapi.api.check_products_consistency(
            product_type, input_product_reference_name, workflow_id="sen2cor_l1c_l2a"
        )


def test_check_products_consistency_wrong_product_type():
    workflow_id = "sen2cor_l1c_l2a"
    product_type = "S2LSI1C"
    input_product_reference_name = (
        "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip"
    )
    with pytest.raises(ValueError, match=f"product type not recognized"):
        esa_tf_restapi.api.check_products_consistency(
            product_type, input_product_reference_name, workflow_id=workflow_id
        )


def test_extract_worflow_options_defaults():

    res = esa_tf_restapi.api.extract_workflow_defaults(WORKFLOW_OPTIONS)

    assert "Name1" in res
    assert res["Name1"] == "VALUE1"

    assert "Name2" in res
    assert res["Name2"] == 1

    assert "Name3" in res
    assert res["Name3"]


def test_fill_with_defaults():

    workflow_options = {"Name3": False, "Name4": 1.4}

    res = esa_tf_restapi.api.fill_with_defaults(workflow_options, WORKFLOW_OPTIONS)

    assert "Name1" in res
    assert res["Name1"] == "VALUE1"

    assert "Name2" in res
    assert res["Name2"] == 1

    assert "Name3" in res
    assert not res["Name3"]

    assert "Name3" in res
    assert res["Name4"] == 1.4


def test_error_fill_with_defaults():

    workflow_options = {"Name3": False}
    with pytest.raises(ValueError, match=r"are missing"):
        esa_tf_restapi.api.fill_with_defaults(workflow_options, WORKFLOW_OPTIONS)
