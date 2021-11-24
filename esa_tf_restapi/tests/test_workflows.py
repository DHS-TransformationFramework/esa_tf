import pytest
from fastapi.testclient import TestClient

from .test_models import register_workflows
from esa_tf_restapi import api, app

client = TestClient(app)


@pytest.fixture()
def workflows():
    orig = api.get_workflows

    def get_workflows():
        return {"sen2cor_l1c_l2a": {}}

    api.get_workflows = get_workflows
    yield
    api.get_workflows = orig


@pytest.fixture()
def workflow():
    orig = api.get_workflow_by_id

    def get_workflow_by_id(id):
        if id == "sen2cor_l1c_l2a":
            return {"Id": id}
        raise KeyError(f"Cannot find {id}")

    api.get_workflow_by_id = get_workflow_by_id
    yield
    api.get_workflow_by_id = orig


@pytest.fixture()
def to_payload():
    orig = api.submit_workflow

    def submit_workflow(id, **kwargs):
        return "foo-bar-baz"

    api.submit_workflow = submit_workflow
    yield
    api.submit_workflow = orig


@pytest.fixture()
def tr_orders():
    orig = api.get_transformation_orders

    def get_transformation_orders(status=None):
        entries = [
            {"Id": "foo", "Status": "in_progress"},
            {"Id": "bar", "Status": "completed"},
        ]
        return [e for e in entries if status is None or e["Status"] == status]

    api.get_transformation_orders = get_transformation_orders
    yield
    api.get_transformation_orders = orig


@pytest.fixture()
def tr_order():
    orig = api.get_transformation_order

    def get_transformation_order(id):
        if id == "foo":
            return {"Id": id}
        raise KeyError(f"Cannot find {id}")

    api.get_transformation_order = get_transformation_order
    yield
    api.get_transformation_order = orig


def test_list_workflows(workflows):
    response = client.get("/Workflows")
    assert response.status_code == 200
    result = response.json()
    assert len(result["value"]) == 1
    assert result["value"][0]["Id"] == "sen2cor_l1c_l2a"


def test_get_workflow(workflow):
    response = client.get("/Workflows('sen2cor_l1c_l2a')")
    assert response.status_code == 200
    result = response.json()
    assert result["Id"] == "sen2cor_l1c_l2a"
    response = client.get("/Workflows('foo-bar')")
    assert response.status_code == 404


def test_list_tranformation_orders(tr_orders):
    response = client.get("/TransformationOrders")
    assert response.status_code == 200
    result = response.json()
    assert len(result["value"]) == 2
    response = client.get("/TransformationOrders?$filter=Status eq 'completed'")
    assert response.status_code == 200
    result = response.json()
    assert len(result["value"]) == 1


def test_get_tranformation_order(tr_order):
    response = client.get("/TransformationOrders('foo')")
    assert response.status_code == 200
    result = response.json()
    assert result["Id"] == "foo"
    response = client.get("/TransformationOrders('bar')")
    assert response.status_code == 404


def test_run_tranformation_order(to_payload, register_workflows):
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
