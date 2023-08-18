import os
import time

import pytest
import requests

S1_TEST = os.getenv(
    "S1_TEST", "S1A_IW_GRDH_1SDV_20230818T062305_20230818T062330_049926_060166_AFA3"
)
S2_TEST = os.getenv(
    "S2_TEST", "S2B_MSIL1C_20230818T022539_N0509_R046_T50NPN_20230818T062210"
)
S3_TEST = os.getenv(
    "S3_TEST",
    "S3A_SY_2_SYN____20230818T080556_20230818T080655_20230818T195414_0059_102_206_3960_PS1_O_ST_002",
)


@pytest.mark.parametrize("reference", [S1_TEST, S2_TEST, S3_TEST])
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


@pytest.mark.parametrize("reference", [S2_TEST, S3_TEST])
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


@pytest.mark.parametrize("reference", [S2_TEST, S3_TEST])
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
