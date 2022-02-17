import logging
from typing import Optional

from fastapi import HTTPException, Query, Request, Response
from fastapi.responses import PlainTextResponse, RedirectResponse, StreamingResponse

from . import api, app, models
from .odata import parse_qs


@app.get("/Workflows")
async def workflows(request: Request):
    # root = request.url_for("metadata")
    data = api.get_workflows()
    return {
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
    # root = request.url_for("metadata")
    return {
        "@odata.id": f"{base}('{id}')",
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
    uri_root = request.url_for("index")
    try:
        data = api.get_transformation_orders(
            [(f.name, f.operator, f.value) for f in filters], uri_root=uri_root,
        )
    except ValueError as exc:
        logging.exception("Invalid request")
        raise HTTPException(status_code=422, detail=str(exc))

    # root = request.url_for("metadata")
    return {
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
    uri_root = request.url_for("index")
    data = None
    try:
        data = api.get_transformation_order(id, uri_root=uri_root)
    except KeyError as exc:
        logging.exception("Invalid Transformation Order id")
        raise HTTPException(status_code=404, detail=str(exc))

    # root = request.url_for("metadata")
    return {
        "@odata.id": f"{base}('{id}')",
        "Id": id,
        **data,
    }


@app.get("/TransformationOrders('{id}')/Log")
async def get_transformation_order_log(id: str):
    try:
        log = api.get_transformation_order_log(id)
    except KeyError as exc:
        logging.exception("Invalid Transformation Order id")
        raise HTTPException(status_code=404, detail=str(exc))
    return {
        "value": log,
    }


@app.get("/TransformationOrders('{id}')/Log/$value", response_class=PlainTextResponse)
async def get_transformation_order_log_raw(id: str):
    log = await get_transformation_order_log(id)
    return "\n".join(log.get("value", []))


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
