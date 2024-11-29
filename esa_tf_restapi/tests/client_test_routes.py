import typing as T
from unittest import mock

import pytest
from fastapi.testclient import TestClient

from esa_tf_restapi import api, app

from .test_models import register_workflows

client = TestClient(app)

TRANSFORMATION_ORDER = {
    "Id": "foo-bar-baz",
    "SubmissionDate": "2021-11-24T15:11:38",
    "InputProductReference": {
        "Reference": "S2B_MSIL1C_20211109T110159_N0301_R094_T29QQB_20211109T114303.zip",
        "DataSourceName": "scihub",
    },
    "WorkflowOptions": {"Aerosol_Type": "RURAL"},
    "WorkflowId": "sen2cor_l1c_l2a",
    "Status": "in_progress",
}

WORKFLOWS = {
    "workflow_1": {
        "Name": "Workflow 1",
        "WorkflowOptions": {
            "Case 1": {
                "Type": "string",
                "Default": "foo",
                "Enum": ["foo", "bar"],
            },
            "Case 2": {
                "Type": "boolean",
                "Default": False,
            },
            "Case 3": {
                "Type": "integer",
                "Default": 331,
                "Enum": [0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460],
            },
            "Case 4": {
                "Type": "integer",
                "Default": 331,
            },
        },
        "Id": "workflow_1",
    }
}


def get_transformation_order(id, uri_root=None, user_id=None):
    if id == "foo":
        return {"Id": id}
    raise api.ItemNotFound(user_id, f"Cannot find {id}")


def get_workflow_by_id(id, user_id):
    if id == "sen2cor_l1c_l2a":
        return {"Id": id}
    raise api.ItemNotFound(user_id, f"Cannot find {id}")


def get_transformation_orders(
    filters: T.List[T.Tuple[str, str, str]] = [], uri_root=None, **kwargs
):
    entries = [
        {"Id": "foo", "Status": "in_progress"},
        {"Id": "bar", "Status": "completed"},
    ]
    for filter in filters:
        name, _op, value = filter
        if name == "Status":
            entries = [e for e in entries if e[name] == value]
    return entries


@mock.patch(
    "esa_tf_restapi.api.get_profile",
    return_value="user",
)
@mock.patch("esa_tf_restapi.api.get_workflows", return_value={"sen2cor_l1c_l2a": {}})
def test_list_workflows(workflows, profile):
    response = client.get("/Workflows")
    assert response.status_code == 200
    result = response.json()
    assert len(result["value"]) == 1
    assert result["value"][0]["Id"] == "sen2cor_l1c_l2a"


@mock.patch(
    "esa_tf_restapi.api.get_profile",
    return_value="user",
)
@mock.patch("esa_tf_restapi.api.get_workflow_by_id", side_effect=get_workflow_by_id)
def test_get_workflow(workflow, profile):
    response = client.get("/Workflows('sen2cor_l1c_l2a')")
    assert response.status_code == 200
    result = response.json()
    assert result["Id"] == "sen2cor_l1c_l2a"
    response = client.get("/Workflows('foo-bar')")
    assert response.status_code == 404


@mock.patch(
    "esa_tf_restapi.api.get_profile",
    return_value="user",
)
@mock.patch(
    "esa_tf_restapi.api.get_transformation_orders",
    side_effect=get_transformation_orders,
)
def test_list_tranformation_orders(tr_orders, profile):
    response = client.get("/TransformationOrders")
    assert response.status_code == 200
    result = response.json()
    assert len(result["value"]) == 2
    response = client.get("/TransformationOrders?$filter=Status eq 'completed'")
    assert response.status_code == 200
    result = response.json()


@mock.patch(
    "esa_tf_restapi.api.get_profile",
    side_effect=["user", "manager"],
)
@mock.patch(
    "esa_tf_restapi.api.get_transformation_orders",
    side_effect=get_transformation_orders,
)
def test_list_admin_tranformation_orders(tr_orders, profile):
    response = client.get("/admin/TransformationOrders")
    assert response.status_code == 403
    response = client.get("/admin/TransformationOrders")
    assert response.status_code == 200


@mock.patch(
    "esa_tf_restapi.api.get_profile",
    return_value="user",
)
@mock.patch(
    "esa_tf_restapi.api.get_transformation_orders",
    side_effect=get_transformation_orders,
)
def test_list_tranformation_orders_count(tr_orders, profile):
    response = client.get("/TransformationOrders/$count")
    assert response.status_code == 200
    result = response.json()
    assert result == 2


@mock.patch(
    "esa_tf_restapi.api.get_profile",
    return_value="user",
)
@mock.patch(
    "esa_tf_restapi.api.get_transformation_order", side_effect=get_transformation_order
)
def test_get_tranformation_order(tr_order, profile):
    response = client.get("/TransformationOrders('foo')")
    assert response.status_code == 200
    result = response.json()
    assert result["Id"] == "foo"
    response = client.get("/TransformationOrders('bar')")
    assert response.status_code == 404


@mock.patch(
    "esa_tf_restapi.api.get_profile",
    return_value="user",
)
@mock.patch(
    "esa_tf_restapi.api.submit_workflow",
    return_value=TRANSFORMATION_ORDER,
)
@mock.patch(
    "esa_tf_restapi.api.get_workflows",
    return_value=WORKFLOWS,
)
def test_run_tranformation_order(to_payload, register_workflows, profile):
    response = client.post(
        "/TransformationOrders",
        json={
            "WorkflowId": "workflow_1",
            "InputProductReference": {"Reference": "foo_bar.zip"},
            "WorkflowOptions": {"Case 1": "bar"},
        },
    )
    assert response.status_code == 201
    assert (
        response.headers["Location"]
        == f"{client.base_url}/TransformationOrders('foo-bar-baz')"
    )
    result = response.json()
    assert result["Id"] == "foo-bar-baz"
