from unittest import mock

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


TRANSFORMATION_ORDERS = {
    "Id1": {
        "Id": "Id1",
        "SubmissionDate": "2022-01-20T16:27:30.000000",
        "CompletedDate": "2022-01-20T16:27:50.000000",
        "Status": "completed",
        "InputProductReference": {"Reference": "product_b"},
    },
    "Id2": {
        "Id": "Id2",
        "SubmissionDate": "2022-01-22T16:27:30.000000",
        "CompletedDate": "2022-01-22T16:27:50.000000",
        "Status": "completed",
        "InputProductReference": {"Reference": "product_a"},
    },
    "Id3": {
        "Id": "Id3",
        "SubmissionDate": "2022-02-01T16:27:30.000000",
        "Status": "in_progress",
        "InputProductReference": {"Reference": "product_b"},
    },
    "Id4": {
        "Id": "Id4",
        "SubmissionDate": "2022-02-02T16:27:30.000000",
        "Status": "in_progress",
        "InputProductReference": {"Reference": "product_a"},
    },
    "Id5": {
        "Id": "Id5",
        "WorkflowId": "sen2cor_l1c_l2a",
        "InputProductReference": {
            "Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip",
            "DataSourceName": "scihub",
        },
        "WorkflowOptions": {},
        "SubmissionDate": "2022-02-02T16:27:30.000000",
        "Status": "failed",
    },
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


@mock.patch(
    "esa_tf_restapi.api.build_transformation_order", side_effect=lambda x: x,
)
def test_get_transformation_orders(function):
    esa_tf_restapi.api.TRANSFORMATION_ORDERS.update(TRANSFORMATION_ORDERS)

    orders = esa_tf_restapi.api.get_transformation_orders([("Id", "eq", "Id1")])

    assert orders == [TRANSFORMATION_ORDERS["Id1"]]
    orders = esa_tf_restapi.api.get_transformation_orders(
        {("Status", "eq", "completed"), ("SubmissionDate", "ge", "2022-01-22")}
    )
    assert set([order["Id"] for order in orders]) == {"Id2"}

    orders = esa_tf_restapi.api.get_transformation_orders(
        {("Status", "eq", "completed"), ("CompletedDate", "le", "2022-02-02")}
    )
    assert set([order["Id"] for order in orders]) == {"Id1", "Id2"}

    orders = esa_tf_restapi.api.get_transformation_orders(
        {("Status", "eq", "in_progress"), ("InputProductReference", "eq", "product_a")}
    )
    assert set([order["Id"] for order in orders]) == {"Id4"}

    orders = esa_tf_restapi.api.get_transformation_orders(
        {
            (
                "InputProductReference",
                "eq",
                "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip",
            )
        }
    )
    assert set([order["Id"] for order in orders]) == {"Id5"}

    with pytest.raises(ValueError, match=r"allowed key"):
        esa_tf_restapi.api.get_transformation_orders(
            {("WrongKey", "op", "value"),}
        )

    with pytest.raises(ValueError, match=r"allowed operator"):
        esa_tf_restapi.api.get_transformation_orders(
            {("Status", "le", "value"),}
        )


def test_check_filter_validity():

    with pytest.raises(ValueError, match=r"allowed key"):
        esa_tf_restapi.api.check_filter_validity([("WrongKey", "op", "value")])

    with pytest.raises(ValueError, match=r"allowed operator"):
        esa_tf_restapi.api.check_filter_validity([("Status", "le", "value")])
