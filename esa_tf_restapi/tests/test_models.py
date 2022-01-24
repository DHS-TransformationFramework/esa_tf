import pytest
from pydantic import ValidationError

from esa_tf_restapi import api, models


@pytest.fixture()
def register_workflows():
    def get_workflows(*args, **kwargs):
        workflows = {
            "workflow_1": {
                "Name": "Workflow 1",
                "WorkflowOptions": {
                    "Case 1": {
                        "Type": "string",
                        "Default": "foo",
                        "Enum": ["foo", "bar"],
                    },
                    "Case 2": {"Type": "boolean", "Default": False,},
                    "Case 3": {
                        "Type": "integer",
                        "Default": 331,
                        "Enum": [0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460],
                    },
                    "Case 4": {"Type": "integer", "Default": 331,},
                },
                "Id": "workflow_1",
            }
        }
        return workflows

    orig = api.get_workflows
    api.get_workflows = get_workflows
    yield
    api.get_workflows = orig


def test_type_checking():
    assert models.type_checking(int, "integer") is True
    assert models.type_checking(bool, "boolean") is True
    assert models.type_checking(float, "number") is True
    assert models.type_checking(str, "string") is True
    assert models.type_checking(str, "str") is False


def test_transformation_order_missing_params(register_workflows):
    with pytest.raises(ValidationError, match=f"field required"):
        models.TranformationOrder()

    with pytest.raises(ValidationError, match=f"field required"):
        models.TranformationOrder(WorkflowId="workflow_1")

    # Checking for registered workflow
    with pytest.raises(ValidationError, match=r"unknown workflow"):
        models.TranformationOrder(
            WorkflowId="workflow_xxxx", InputProductReference={"Reference": "Ref a"},
        )

    # Testing workflow options can be provided as empty dict
    try:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={},
        )
    except:
        pytest.fail("Empty WorkflowOptions should be allowed")


def test_transformation_order_validate_workflow_options(register_workflows):
    with pytest.raises(ValueError, match=f"invalid parameter"):
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 99": "foo"},
        )

    with pytest.raises(ValueError, match=f"disallowed value"):
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 1": "baz"},
        )

    # Valid Case 1
    try:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 1": "foo"},
        )
    except:
        pytest.fail("Valid Case 1 failed")

    with pytest.raises(ValidationError, match=r"wrong type"):
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 2": "foo"},
        )

    # Valid Case 2
    try:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 2": True},
        )
    except:
        pytest.fail("Valid Case 2 failed")

    with pytest.raises(ValidationError, match=f"disallowed value"):
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 3": 999},
        )


    # Valid Case 2
    try:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 3": 410},
        )
    except:
        pytest.fail("Valid Case 3 failed")
