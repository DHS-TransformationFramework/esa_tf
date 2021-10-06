from fastapi import FastAPI
from fastapi.testclient import TestClient

from esa_tf_restapi import app

client = TestClient(app)


def test_list_workflows():
    response = client.get("/Workflows")
    assert response.status_code == 200
    res = response.json()
    assert len(res["value"]) == 1
