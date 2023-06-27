import os
import time

import requests

REFERENCE = os.getenv(
    "REFERENCE", "S2A_MSIL1C_20230305T075811_N0509_R035_T36MVU_20230305T094419"
)


def test_eopf_convert_to_zarr():
    process_prams = {
        "WorkflowId": "eopf_convert_to_zarr",
        "InputProductReference": {
            "Reference": REFERENCE,
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


def test_eopf_convert_to_netcdf():
    process_prams = {
        "WorkflowId": "eopf_convert_to_netcdf",
        "InputProductReference": {
            "Reference": REFERENCE,
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


def test_eopf_convert_to_cog():
    process_prams = {
        "WorkflowId": "eopf_convert_to_cog",
        "InputProductReference": {
            "Reference": REFERENCE,
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
