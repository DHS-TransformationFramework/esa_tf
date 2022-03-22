import logging
from typing import Optional

from fastapi import Header, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, PlainTextResponse

from . import api, app, models
from .auth import DEFAULT_USER, get_user
from .odata import parse_qs

logger = logging.getLogger(__name__)


@app.get("/", status_code=status.HTTP_404_NOT_FOUND, response_class=HTMLResponse)
async def index():
    pass


@app.get("/Workflows")
async def workflows(
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    user = get_user(x_username, x_roles)
    user_id = user.username if user else DEFAULT_USER
    logger.info(f"required workflows configurations", extra=dict(user=user_id))
    data = api.get_workflows()
    return {
        "value": [{"Id": id, **ops} for id, ops in data.items()],
    }


@app.get("/Workflows('{id}')", name="workflow")
async def workflow(
    request: Request,
    id: str,
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    user = get_user(x_username, x_roles)
    user_id = user.username if user else DEFAULT_USER
    logger.info(
        f"required the configuration about '{id}' workflow", extra=dict(user=user_id),
    )
    data = {}
    try:
        data = api.get_workflow_by_id(id)
    except KeyError as exc:
        logging.exception("Invalid Worfklow id")
        raise HTTPException(status_code=404, detail=str(exc))
    base = request.url_for("workflows")
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
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    user = get_user(x_username, x_roles)
    user_id = user.username if user else DEFAULT_USER
    odata_params = parse_qs(filter=rawfilter)
    filters = [(f.name, f.operator, f.value) for f in odata_params.filter]
    if not count:
        msg = f"required the transformation orders list"
        msg_filter = ""
        if filters:
            msg_filter = (
                f" filtered by '{' and '.join([' '.join(f) for f in filters])}'"
            )
        logger.info(msg + msg_filter, extra=dict(user=user_id))
    uri_root = request.url_for("index")
    try:
        data = api.get_transformation_orders(filters)
    except ValueError as exc:
        logging.exception("Invalid request")
        raise HTTPException(status_code=422, detail=str(exc))

    return {
        **({"odata.count": len(data)} if count else {}),
        "value": data,
    }


@app.get("/TransformationOrders/$count")
async def transformation_orders_count(
    request: Request,
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    user = get_user(x_username, x_roles)
    user_id = user.username if user else DEFAULT_USER
    logger.info("required the transformation orders count", extra=dict(user=user_id))
    results = await transformation_orders(
        request, rawfilter=None, count=True, x_username=x_username, x_roles=x_roles
    )
    return results["odata.count"]


@app.get("/TransformationOrders('{id}')", name="transformation_order")
async def get_transformation_order(
    request: Request,
    id: str,
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    user = get_user(x_username, x_roles)
    user_id = user.username if user else DEFAULT_USER
    logger.info(
        f"required info about the transformation order '{id}'", extra=dict(user=user_id)
    )
    base = request.url_for("transformation_orders")
    uri_root = request.url_for("index")
    data = None
    try:
        data = api.get_transformation_order(id)
    except KeyError as exc:
        logging.exception("Invalid Transformation Order id")
        raise HTTPException(status_code=404, detail=str(exc))

    return {
        "@odata.id": f"{base}('{id}')",
        "Id": id,
        **data,
    }


@app.get("/TransformationOrders('{id}')/Log")
async def get_transformation_order_log(
    id: str,
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    user = get_user(x_username, x_roles)
    user_id = user.username if user else DEFAULT_USER
    logger.info(
        f"required the log-file for the transformation order '{id}'",
        extra=dict(user=user_id),
    )
    try:
        log = api.get_transformation_order_log(id)
    except KeyError as exc:
        logging.exception("Invalid Transformation Order id")
        raise HTTPException(status_code=404, detail=str(exc))
    return {
        "value": log,
    }


@app.get("/TransformationOrders('{id}')/Log/$value", response_class=PlainTextResponse)
async def get_transformation_order_log_raw(
    id: str,
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    log = await get_transformation_order_log(id, x_username, x_roles)
    return "\n".join(log.get("value", []))


@app.post("/TransformationOrders", status_code=201)
async def transformation_order_create(
    request: Request,
    response: Response,
    data: models.TranformationOrder,
    x_username: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None),
):
    uri_root = request.url_for("index")
    user = get_user(x_username, x_roles)
    user_id = user.username if user else DEFAULT_USER
    running_transformation = None
    try:
        running_transformation = api.submit_workflow(
            data.workflow_id,
            input_product_reference=data.product_reference.dict(
                by_alias=True, exclude_unset=True
            ),
            workflow_options=data.workflow_options,
            user_id=user_id,
            user_roles=user.roles if user_id != DEFAULT_USER else None,
            uri_root=uri_root,
        )
    except ValueError as exc:
        logging.exception("Invalid Transformation Order")
        raise HTTPException(status_code=422, detail=str(exc))
    url = request.url_for("transformation_order", id=running_transformation.get("Id"))
    response.headers["Location"] = url
    return {**running_transformation}
