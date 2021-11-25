import pytest
from pydantic import ValidationError

from esa_tf_restapi import api, models


@pytest.fixture()
def register_workflows():
    def get_workflows(*args, **kwargs):
        workflows = {
            "workflow_1": {
                "Name": "Workflow 1",
                "WorkflowOptions": [
                    {
                        "Name": "Case 1",
                        "Type": "string",
                        "Default": "foo",
                        "Enum": ["foo", "bar"],
                    },
                    {"Name": "Case 2", "Type": "boolean", "Default": False,},
                    {
                        "Name": "Case 3",
                        "Type": "integer",
                        "Default": 331,
                        "Enum": [0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460],
                    },
                    {"Name": "Case 4", "Type": "integer", "Default": 331,},
                ],
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
    with pytest.raises(ValidationError) as excinfo:
        models.TranformationOrder()
    assert "WorkflowId\n  field required (type=value_error.missing)" in str(
        excinfo.value
    )

    with pytest.raises(ValidationError) as excinfo:
        models.TranformationOrder(WorkflowId="workflow_1")
    assert "InputProductReference\n  field required (type=value_error.missing)" in str(
        excinfo.value
    )

    # Checking for registered workflow
    with pytest.raises(ValidationError) as excinfo:
        models.TranformationOrder(
            WorkflowId="workflow_xxxx", InputProductReference={"Reference": "Ref a"},
        )
    assert (
        "WorkflowId\n  unknown workflow: workflow_xxxx. Registered workflows are: workflow_1 (type=value_error)"
        in str(excinfo.value)
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
    with pytest.raises(ValidationError) as excinfo:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 99": "foo"},
        )
    assert (
        "WorkflowOptions\n  Case 99 is an unknown name for workflow_1 plugin. "
        "Possible names are Case 1, Case 2, Case 3, Case 4 (type=value_error)"
        in str(excinfo.value)
    )

    with pytest.raises(ValidationError) as excinfo:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 1": "baz"},
        )
    assert (
        "WorkflowOptions\n  disallowed value for Case 1: baz has been provided while possible values are foo, bar (type=value_error)"
        in str(excinfo.value)
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

    with pytest.raises(ValidationError) as excinfo:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 2": "foo"},
        )
    assert (
        "WorkflowOptions\n  wrong type for Case 2. Param type should be boolean while 'foo' (of type str) provided (type=value_error)"
        in str(excinfo.value)
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

    with pytest.raises(ValidationError) as excinfo:
        models.TranformationOrder(
            WorkflowId="workflow_1",
            InputProductReference={"Reference": "Ref a"},
            WorkflowOptions={"Case 3": 999},
        )
    assert (
        "WorkflowOptions\n  disallowed value for Case 3: 999 has been provided while possible values are 0, 250, 290, 330, 331, 370, 377, 410, 420, 450, 460 (type=value_error)"
        in str(excinfo.value)
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
