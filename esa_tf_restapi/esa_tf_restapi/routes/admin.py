# Copyright 2021-2022, European Space Agency (ESA)
#
# Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE Version 3 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://opensource.org/licenses/AGPL-3.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.responses import HTMLResponse, PlainTextResponse

from ..dependencies import get_roles_header
from . import transformation_orders

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(get_roles_header)],
    responses={404: {"description": "Not found"}},
)


@router.get("/TransformationOrders")
async def admin_transformation_orders(
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
    return await transformation_orders(request, rawfilter, count, x_username, x_roles)
