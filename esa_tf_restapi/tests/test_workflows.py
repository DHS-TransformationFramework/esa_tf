import pytest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from esa_tf_restapi import app
from esa_tf_restapi import api

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
        return {"Id": id}

    api.get_workflow_by_id = get_workflow_by_id
    yield
    api.get_workflow_by_id = orig


def test_list_workflows(workflows):
    response = client.get("/Workflows")
    assert response.status_code == 200
    res = response.json()
    assert len(res["value"]) == 1
    assert res["value"][0]["Id"] == "sen2cor_l1c_l2a"


def test_get_workflow(workflow):
    response = client.get("/Workflows('lorem-ipsum')")
    assert response.status_code == 200
    res = response.json()
    assert res["Id"] == "lorem-ipsum"
