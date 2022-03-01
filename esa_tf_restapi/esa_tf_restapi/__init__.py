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

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from odata_query.exceptions import ODataException

__version__ = "0.8.2"

app = FastAPI(root_path=os.environ.get("ROOT_PATH", ""))

from . import logger_setup, routes

logger_setup.logger_setup()


@app.exception_handler(ODataException)
async def validation_exception_handler(request, exc):
    logging.exception("Invalid OData query")
    return JSONResponse(content={"detail": "Invalid OData query"}, status_code=422,)
