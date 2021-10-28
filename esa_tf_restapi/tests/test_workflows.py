import pytest
from fastapi.testclient import TestClient

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


def test_list_workflows(workflows):
    response = client.get("/Workflows")
    assert response.status_code == 200
    res = response.json()
    assert len(res["value"]) == 1
    assert res["value"][0]["Id"] == "sen2cor_l1c_l2a"


def test_get_workflow(workflow):
    response = client.get("/Workflows('sen2cor_l1c_l2a')")
    assert response.status_code == 200
    res = response.json()
    assert res["Id"] == "sen2cor_l1c_l2a"
    response = client.get("/Workflows('foo-bar')")
    assert response.status_code == 404


def test_run_tranformation_order(to_payload):
    response = client.post(
        "/TransformationOrders",
        json={
            "WorkflowId": "sen2cor_l1c_l2a",
            "InputProductReference": {"Reference": "foo_bar.zip"},
            "WorkflowOptions": {},
        },
    )
    assert response.status_code == 201
    assert (
        response.headers["Location"]
        == f"{client.base_url}/TransformationOrders('foo-bar-baz')"
    )
    res = response.json()
    assert res["Id"] == "foo-bar-baz"
