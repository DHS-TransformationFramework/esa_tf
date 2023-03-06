import logging
from typing import Optional

from pydantic import BaseModel, Field, validator

from . import api

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
    workflow_options: Optional[dict] = Field(alias="WorkflowOptions", default={})

    @validator("workflow_id", always=True, pre=True)
    def validate_wf_id(cls, v, values):
        workflows = api.get_workflows()
        workflows_ids = list(workflows)

        if v not in workflows_ids:
            exc = ValueError(
                f"unknown workflow: {v!r}. Registered workflows are: {workflows_ids!r}"
            )
            logging.exception("Unknown Workflow id", exc_info=exc)
            raise exc
        return v

    @validator("workflow_options")
    def validate_wf_options(cls, v, values):
        try:
            workflows = api.get_workflows()
        except ValueError as exc:
            # This is needed because we don't want to capture internal pydantic exceptions
            raise Exception(exc.args[0]) from exc
        workflow_id = values.get("workflow_id")
        # workflow id check has been done previously
        # if workflow_id is None there is no point in checking the workflow options.
        if not workflow_id:
            return v

        workflow = workflows.get(workflow_id, {})
        workflow_options = workflow.get("WorkflowOptions")

        # Check for possible W.O. name
        possible_wo_names = workflow_options.keys()
        for key in v.keys():
            if key not in possible_wo_names:
                exc = ValueError(
                    f"{key!r} is an unknown parameter for {workflow_id!r} workflow. "
                    f"Possible parameters are {possible_wo_names!r}"
                )
                logging.exception("Unknown parameter", exc_info=exc)
                raise exc

        # Check for proper types (integer, boolean, string, number, …)
        for key, value in v.items():
            current_option = workflow_options[key]
            if not type_checking(type(value), current_option["Type"]):
                exc = ValueError(
                    f"wrong type for {key!r}. "
                    f"Param type should be {current_option['Type']!r} "
                    f"while {value!r} (of type {type(value).__name__}) provided"
                )
                logging.exception("Wrong data type", exc_info=exc)
                raise exc

        # Check for one o possible values used (when "Enum" is provided)
        for key, value in v.items():
            current_option = workflow_options[key]
            if "Enum" not in current_option:
                continue
            if value not in current_option["Enum"]:
                exc = ValueError(
                    f"disallowed value for {key!r}: "
                    f"{value!r} has been provided while possible values are "
                    f"{current_option['Enum']!r}"
                )
                logging.exception("Disallowed value", exc_info=exc)
                raise exc

        return v
