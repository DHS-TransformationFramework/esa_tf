from typing import Optional
from pydantic import BaseModel, Field


class ContentDate(BaseModel):
    start: str = Field(alias="Start")
    end: str = Field(alias="End")


class ProductReference(BaseModel):
    reference: str = Field(alias="Reference")
    content_date: Optional[ContentDate] = Field(None, alias="ContentDate")


class TranformationOrder(BaseModel):
    workflow_id: str = Field(alias="WorkflowId")
    product_reference: ProductReference = Field(alias="InputProductReference")
    workflow_options: dict = Field(alias="WorkflowOptions")
