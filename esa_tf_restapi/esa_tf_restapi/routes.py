from typing import Optional

from fastapi import HTTPException, Query, Request, Response

from . import api, app, models
from .csdl import loadDefinition
from .odata import parseQS


@app.get("/$metadata")
async def metadata():
    return loadDefinition()


@app.get("/Workflows")
async def workflows(request: Request):
    base = request.url_for("workflows")
    data = api.get_workflows()
    return {
        "@odata.context": f"{base}/$metadata#Workflow",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "value": [{**ops, "Id": id} for id, ops in data.items()],
    }


@app.get("/Workflows('{id}')", name="workflow")
async def workflow(request: Request, id: str):
    data = {}
    try:
        data = api.get_workflow_by_id(id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")
    base = request.url_for("workflow", id=id)
    return {
        "@odata.id": f"{base}/Workflows('{id}')",
        "@odata.context": f"{base}/$metadata#Workflow('{id}')",
        "Id": id,
        **data,
    }


@app.get("/TransformationOrders")
async def tranformation_orders(
    request: Request,
    rawfilter: Optional[str] = Query(
        None, alias="$filter", title="OData $filter query", max_length=50
    ),
):
    odata_params = parseQS(filter=rawfilter)
    filter = odata_params.filter
    data = api.get_transformation_orders(status=filter.value)
    base = request.url_for("tranformation_orders")
    return {
        "@odata.context": f"{base}/$metadata#TransformationOrder",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "value": data,
    }


@app.get("/TransformationOrders('{id}')", name="transformation_order")
async def transformation_order(request: Request, id: str):
    base = request.url_for("transformation_order", id=id)
    data = None
    try:
        data = api.get_order_status(id)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Transformation order {id} not found"
        )
    return {
        "@odata.id": f"{base}/TransformationOrders('{id}')",
        "@odata.context": f"{base}/$metadata",
        # "@odata.nextLink": "https://services.odata.org/V4/TripPinService/People?%24select=FirstName&%24skiptoken=8",
        "Id": id,
        **data,
    }


@app.post("/TransformationOrders", status_code=201)
async def transformation_order_create(
    request: Request, response: Response, data: models.TranformationOrder,
):
    id = api.submit_workflow(
        data.workflow_id,
        input_product_reference=data.product_reference.dict(
            by_alias=True, exclude_unset=True
        ),
        workflow_options=data.workflow_options,
    )
    url = request.url_for("transformation_order", id=id)
    response.headers["Location"] = url
    return {
        "Id": id,
    }
