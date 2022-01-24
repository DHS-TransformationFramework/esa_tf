from typing import Optional

from fastapi import HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse, StreamingResponse

from . import api, app, models
from .csdl import loadDefinition
from .odata import parse_qs


@app.get("/")
async def index():
    return RedirectResponse("/$metadata")


@app.get("/$metadata")
def metadata():
    return StreamingResponse(loadDefinition(), media_type="application/xml")


@app.get("/Workflows")
async def workflows(request: Request):
    root = request.url_for("metadata")
    data = api.get_workflows()
    return {
        "@odata.context": f"{root}#Workflows",
        "value": [{"Id": id, **ops} for id, ops in data.items()],
    }


@app.get("/Workflows('{id}')", name="workflow")
async def workflow(request: Request, id: str):
    data = {}
    try:
        data = api.get_workflow_by_id(id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")
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
        None, alias="$filter", title="OData $filter query",
    ),
    count: Optional[str] = Query(
        False,
        alias="$count",
        title="OData $count flag",
        description='Include number of results in the "odata.count" field',
    ),
):
    odata_params = parse_qs(filter=rawfilter)
    filters = odata_params.filter
    data = api.get_transformation_orders(
        [(f.name, f.operator, f.value) for f in filters]
    )
    root = request.url_for("metadata")
    return {
        "@odata.context": f"{root}#TransformationOrders",
        **({"odata.count": len(data)} if count else {}),
        "value": data,
    }


@app.get("/TransformationOrders/$count")
async def transformation_orders_count(request: Request,):
    results = await transformation_orders(request, rawfilter=None, count=True)
    return results["odata.count"]


@app.get("/TransformationOrders('{id}')", name="transformation_order")
async def get_transformation_order(request: Request, id: str):
    base = request.url_for("transformation_orders")
    data = None
    try:
        data = api.get_transformation_order(id)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Transformation order {id} not found"
        )
    root = request.url_for("metadata")
    return {
        "@odata.id": f"{base}('{id}')",
        "@odata.context": f"{root}#TransformationOrders('{id}')",
        "Id": id,
        **data,
    }


@app.post("/TransformationOrders", status_code=201)
async def transformation_order_create(
    request: Request, response: Response, data: models.TranformationOrder,
):
    running_transformation = api.submit_workflow(
        data.workflow_id,
        input_product_reference=data.product_reference.dict(
            by_alias=True, exclude_unset=True
        ),
        workflow_options=data.workflow_options,
    )
    url = request.url_for("transformation_order", id=running_transformation.get("Id"))
    response.headers["Location"] = url
    return {**running_transformation}
