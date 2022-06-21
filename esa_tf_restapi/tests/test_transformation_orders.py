import datetime
from unittest import mock

import esa_tf_restapi

TO_KWARGS = {
    "client": None,
    "order_id": None,
    "product_reference": None,
    "workflow_id": None,
    "workflow_options": None,
}


TRANSFORMATION_ORDERS_USER1 = {
    "Id1": esa_tf_restapi.transformation_orders.TransformationOrder(**TO_KWARGS),
    "Id2": esa_tf_restapi.transformation_orders.TransformationOrder(**TO_KWARGS),
}

TRANSFORMATION_ORDERS_USER2 = {
    "Id3": esa_tf_restapi.transformation_orders.TransformationOrder(**TO_KWARGS),
    "Id4": esa_tf_restapi.transformation_orders.TransformationOrder(**TO_KWARGS),
}

TRANSFORMATION_ORDERS_USER3 = {
    "Id3": esa_tf_restapi.transformation_orders.TransformationOrder(**TO_KWARGS),
    "Id5": esa_tf_restapi.transformation_orders.TransformationOrder(**TO_KWARGS),
}

TRANSFORMATION_ORDERS_USER1["Id1"]._info = {
    "Id": "Id1",
    "SubmissionDate": "2022-01-20T16:27:30.000000",
    "CompletedDate": "2022-01-20T16:27:50.000000",
    "Status": "completed",
    "InputProductReference": {"Reference": "product_b"},
}
TRANSFORMATION_ORDERS_USER1["Id2"]._info = {
    "Id": "Id2",
    "SubmissionDate": "2022-01-22T16:27:30.000000",
    "CompletedDate": "2022-01-22T16:37:50.000000",
    "Status": "completed",
    "InputProductReference": {"Reference": "product_a"},
}
TRANSFORMATION_ORDERS_USER2["Id3"]._info = {
    "Id": "Id3",
    "SubmissionDate": "2022-02-01T16:27:30.000000",
    "Status": "in_progress",
    "InputProductReference": {"Reference": "product_b"},
}
TRANSFORMATION_ORDERS_USER2["Id4"]._info = {
    "Id": "Id4",
    "SubmissionDate": "2022-02-02T16:27:30.000000",
    "Status": "in_progress",
    "InputProductReference": {"Reference": "product_a"},
}
TRANSFORMATION_ORDERS_USER3["Id5"]._info = {
    "Id": "Id5",
    "WorkflowId": "sen2cor_l1c_l2a",
    "InputProductReference": {
        "Reference": "S2A_MSIL1C_20211022T062221_N0301_R048_T39GWH_20211022T064132.zip",
        "DataSourceName": "scihub",
    },
    "WorkflowOptions": {},
    "SubmissionDate": "2022-02-02T16:27:30.000000",
    "Status": "failed",
}
TRANSFORMATION_ORDERS_USER3["Id3"]._info = TRANSFORMATION_ORDERS_USER2["Id3"]._info


@mock.patch(
    "esa_tf_restapi.api.TransformationOrder.update_status", side_effect=None,
)
def test_queue_update_orders(function):
    queue = esa_tf_restapi.transformation_orders.Queue()

    queue.update_orders(TRANSFORMATION_ORDERS_USER1.values(), user_id="user_1")
    assert len(queue.transformation_orders) == 2
    assert queue.user_to_orders == {"user_1": {"Id1", "Id2"}}
    assert queue.order_to_users == {"Id1": {"user_1"}, "Id2": {"user_1"}}

    queue.update_orders(TRANSFORMATION_ORDERS_USER2.values(), user_id="user_2")
    assert len(queue.transformation_orders) == 4
    assert queue.user_to_orders == {"user_1": {"Id1", "Id2"}, "user_2": {"Id3", "Id4"}}
    assert queue.order_to_users == {
        "Id1": {"user_1"},
        "Id2": {"user_1"},
        "Id3": {"user_2"},
        "Id4": {"user_2"},
    }

    queue.update_orders(TRANSFORMATION_ORDERS_USER3.values(), user_id="user_3")
    assert len(queue.transformation_orders) == 5
    assert queue.user_to_orders == {
        "user_1": {"Id1", "Id2"},
        "user_2": {"Id3", "Id4"},
        "user_3": {"Id3", "Id5"},
    }
    assert queue.order_to_users == {
        "Id1": {"user_1"},
        "Id2": {"user_1"},
        "Id3": {"user_2", "user_3"},
        "Id4": {"user_2"},
        "Id5": {"user_3"},
    }


@mock.patch(
    "esa_tf_restapi.api.TransformationOrder.update_status", side_effect=None,
)
def test_queue_remove_order(function):
    queue = esa_tf_restapi.transformation_orders.Queue()
    queue.update_orders(TRANSFORMATION_ORDERS_USER1.values(), user_id="user_1")
    queue.update_orders(TRANSFORMATION_ORDERS_USER2.values(), user_id="user_2")
    queue.update_orders(TRANSFORMATION_ORDERS_USER3.values(), user_id="user_3")

    assert len(queue.transformation_orders) == 5

    order_id = "Id3"
    queue.remove_order(order_id)
    assert order_id not in queue.transformation_orders
    assert len(queue.transformation_orders) == 4
    assert queue.user_to_orders == {
        "user_1": {"Id1", "Id2"},
        "user_2": {"Id4"},
        "user_3": {"Id5"},
    }
    assert queue.order_to_users == {
        "Id1": {"user_1"},
        "Id2": {"user_1"},
        "Id4": {"user_2"},
        "Id5": {"user_3"},
    }


@mock.patch(
    "esa_tf_restapi.api.TransformationOrder.update_status", side_effect=None,
)
def test_queue_remove_old_orders(function):
    queue = esa_tf_restapi.transformation_orders.Queue()
    queue.update_orders(TRANSFORMATION_ORDERS_USER1.values(), user_id="user_1")
    queue.update_orders(TRANSFORMATION_ORDERS_USER2.values(), user_id="user_2")
    queue.update_orders(TRANSFORMATION_ORDERS_USER3.values(), user_id="user_3")

    assert len(queue.transformation_orders) == 5

    keeping_period = 10  #  minutes
    now = datetime.datetime(2022, 1, 20, 16, 40)
    queue.remove_old_orders(keeping_period, reference_time=now)
    assert "Id1" not in queue.transformation_orders
    assert len(queue.transformation_orders) == 4
    assert queue.user_to_orders == {
        "user_1": {"Id2"},
        "user_2": {"Id3", "Id4"},
        "user_3": {"Id3", "Id5"},
    }
    assert queue.order_to_users == {
        "Id2": {"user_1"},
        "Id3": {"user_2", "user_3"},
        "Id4": {"user_2"},
        "Id5": {"user_3"},
    }
