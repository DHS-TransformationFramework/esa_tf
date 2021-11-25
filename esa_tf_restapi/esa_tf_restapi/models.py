from typing import Optional

from pydantic import BaseModel, Field, validator
from pydantic.errors import MissingError

TYPES = {
    "boolean": bool,
    "number": float,
    "integer": int,
    "string": str,
}


def type_checking(param_type, wf_opt_type):
    return TYPES.get(wf_opt_type) == param_type


class ContentDate(BaseModel):
    start: str = Field(alias="Start")
    end: str = Field(alias="End")


class ProductReference(BaseModel):
    reference: str = Field(alias="Reference")
    data_source_name: Optional[str] = Field(None, alias="DataSourceName")
    content_date: Optional[ContentDate] = Field(alias="ContentDate")


class TranformationOrder(BaseModel):
    workflow_id: str = Field(alias="WorkflowId")
    product_reference: ProductReference = Field(alias="InputProductReference")
    workflow_options: Optional[dict] = Field(alias="WorkflowOptions")

    @validator("workflow_id", always=True)
    def validate_wf_id(cls, v, values):
        from . import workflows

        workflows_ids = workflows.keys()

        if v not in workflows_ids:
            raise ValueError(
                f"unknown workflow: {v}. Registered workflows are: {', '.join(workflows_ids)}"
            )
        return v

    @validator("workflow_options")
    def validate_wf_options(cls, v, values):
        from . import workflows

        workflow_id = values.get("workflow_id")
        workflow = workflows.get(workflow_id, {})
        workflow_options = {opt["Name"]: opt for opt in workflow.get("WorkflowOptions")}

        # Check for possible W.O. name
        possible_wo_names = workflow_options.keys()
        for key in v.keys():
            if key not in possible_wo_names:
                raise ValueError(
                    f"{key} is an unknown name for {workflow_id} plugin. "
                    f"Possible names are {', '.join(possible_wo_names)}"
                )

        # Check for proper types (integer, boolean, string, number, …)
        for key, value in v.items():
            current_option = workflow_options[key]
            if not type_checking(type(value), current_option["Type"]):
                raise ValueError(
                    f"wrong type for {key}. "
                    f"Param type should be {current_option['Type']} "
                    f"while {repr(value)} (of type {type(value).__name__}) provided"
                )

        # Check for one o possible values used (when "Enum" is provided)
        for key, value in v.items():
            current_option = workflow_options[key]
            if "Enum" not in current_option:
                continue
            if value not in current_option["Enum"]:
                raise ValueError(
                    f"disallowed value for {key}: "
                    f"{value} has been provided while possible values are "
                    f"{', '.join([str(x) for x in current_option['Enum']])}"
                )
        return v
