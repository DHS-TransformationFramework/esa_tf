import os
import pytest
import time

import requests


REFERENCES = [
    "S2A_MSIL1C_20230305T075811_N0509_R035_T36MVU_20230305T094419",
    "S3A_OL_1_EFR____20230703T125831_20230703T125941_20230704T131848_0070_100_323_3780_PS1_O_NT_003"
]


@pytest.mark.parametrize("reference", REFERENCES)
def test_eopf_convert_to_zarr(reference):
    process_prams = {
        "WorkflowId": "eopf_convert_to_zarr",
        "InputProductReference": {
            "Reference": reference,
        },
    }

    order_submission = requests.post(
        "http://localhost:8080/TransformationOrders",
        json=process_prams,
    )

    order_id = order_submission.json()["Id"]

    k = 0
    while k < 1000:
        order = requests.get(
            f"http://localhost:8080/TransformationOrders('{order_id}')"
        )
        order_status = order.json()["Status"]
        if order_status != "in_progress":
            break
        time.sleep(10)

    assert order_status == "completed"


@pytest.mark.parametrize("reference", REFERENCES)
def test_eopf_convert_to_netcdf(reference):
    process_prams = {
        "WorkflowId": "eopf_convert_to_netcdf",
        "InputProductReference": {
            "Reference": reference,
        },
    }

    order_submission = requests.post(
        "http://localhost:8080/TransformationOrders",
        json=process_prams,
    )

    order_id = order_submission.json()["Id"]

    k = 0
    while k < 1000:
        order = requests.get(
            f"http://localhost:8080/TransformationOrders('{order_id}')"
        )
        order_status = order.json()["Status"]
        if order_status != "in_progress":
            break
        time.sleep(10)

    assert order_status == "completed"


@pytest.mark.parametrize("reference", REFERENCES)
def test_eopf_convert_to_cog(reference):
    process_prams = {
        "WorkflowId": "eopf_convert_to_cog",
        "InputProductReference": {
            "Reference": reference,
        },
    }

    order_submission = requests.post(
        "http://localhost:8080/TransformationOrders",
        json=process_prams,
    )

    order_id = order_submission.json()["Id"]

    k = 0
    while k < 1000:
        order = requests.get(
            f"http://localhost:8080/TransformationOrders('{order_id}')"
        )
        order_status = order.json()["Status"]
        if order_status != "in_progress":
            break
        time.sleep(10)

    assert order_status == "completed"
