from typing import Optional

from pydantic import BaseModel, Field


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
    workflow_options: dict = Field(alias="WorkflowOptions")
