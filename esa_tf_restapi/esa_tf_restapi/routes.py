import logging
from typing import Optional

from fastapi import HTTPException, Query, Request, Response

from . import api, app, models
from .csdl import loadDefinition
from .odata import parse_qs


@app.get("/")
@app.get("/$metadata")
async def metadata():
    return loadDefinition()


@app.get("/Workflows")
async def workflows(request: Request):
    root = request.url_for("metadata")
    data = api.get_workflows()
    return {
        "@odata.context": f"{root}#Workflows",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "value": [{"Id": id, **ops} for id, ops in data.items()],
    }


@app.get("/Workflows('{id}')", name="workflow")
async def workflow(request: Request, id: str):
    data = {}
    try:
        data = api.get_workflow_by_id(id)
    except KeyError as exc:
        logging.exception("Invalid Worfklow id")
        raise HTTPException(status_code=404, detail=str(exc))
    base = request.url_for("workflows")
    root = request.url_for("metadata")
    return {
        "@odata.id": f"{base}('{id}')",
        "@odata.context": f"{root}#Workflows('{id}')",
        "Id": id,
        **data,
    }


@app.get("/TransformationOrders")
async def transformation_orders(
    request: Request,
    rawfilter: Optional[str] = Query(
        None, alias="$filter", title="OData $filter query", max_length=50
    ),
):
    odata_params = parse_qs(filter=rawfilter)
    filter = odata_params.filter
    data = api.get_transformation_orders(status=filter.value)
    root = request.url_for("metadata")
    return {
        "@odata.context": f"{root}#TransformationOrders",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "value": data,
    }


@app.get("/TransformationOrders('{id}')", name="transformation_order")
async def get_transformation_order(request: Request, id: str):
    base = request.url_for("transformation_orders")
    data = None
    try:
        data = api.get_transformation_order(id)
    except KeyError as exc:
        logging.exception("Invalid Transformation Order id")
        raise HTTPException(status_code=404, detail=str(exc))

    root = request.url_for("metadata")
    return {
        "@odata.id": f"{base}('{id}')",
        "@odata.context": f"{root}#TransformationOrders('{id}')",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "Id": id,
        **data,
    }


@app.post("/TransformationOrders", status_code=201)
async def transformation_order_create(
    request: Request, response: Response, data: models.TranformationOrder,
):
    running_transformation = None
    try:
        running_transformation = api.submit_workflow(
            data.workflow_id,
            input_product_reference=data.product_reference.dict(
                by_alias=True, exclude_unset=True
            ),
            workflow_options=data.workflow_options,
        )
    except ValueError as exc:
        logging.exception("Invalid Transformation Order")
        raise HTTPException(status_code=422, detail=str(exc))
    url = request.url_for("transformation_order", id=running_transformation.get("Id"))
    response.headers["Location"] = url
    return {**running_transformation}
