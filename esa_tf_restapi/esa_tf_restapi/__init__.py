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

import logging
import os

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from odata_query.exceptions import ODataException

__version__ = "1.1.1-osf"

app = FastAPI(root_path=os.environ.get("ROOT_PATH", ""))

from . import api, logger_setup, routes

logger_setup.logger_setup()


@app.exception_handler(ODataException)
async def validation_exception_handler(request, exc):
    logging.exception("Invalid OData query")
    return JSONResponse(content={"detail": "Invalid OData query"}, status_code=422,)


@app.exception_handler(api.RequestError)
async def request_exception_handler(request, exc):
    logging.exception(f"user: {exc.user_id!r}: invalid request")
    return JSONResponse(content={"detail": str(exc)}, status_code=422,)


@app.exception_handler(api.ItemNotFound)
async def key_exception_handler(request, exc):
    logging.exception(f"user: {exc.user_id!r}: item not found")
    return JSONResponse(content={"detail": str(exc)}, status_code=404,)


@app.exception_handler(api.ExceededQuota)
async def quota_exception_handler(request, exc):
    logging.exception(f"user: {exc.user_id!r}: exceeded user quota")
    return JSONResponse(content={"detail": str(exc)}, status_code=429,)


@app.exception_handler(Exception)
async def validation_exception_handler(request, exc):
    logging.exception("internal server error")
    return JSONResponse(content={"detail": "internal server error"}, status_code=500,)
