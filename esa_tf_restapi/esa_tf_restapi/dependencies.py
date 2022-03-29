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
import typing as T

from fastapi import Header, HTTPException, status

from . import api, auth


async def role_has_manager_profile(
    x_username: T.Optional[str] = Header(None), x_roles: str = Header(None)
):
    user = auth.get_user(x_username, x_roles)
    profile = api.get_profile(user_roles=user.roles, user_id=user.username)
    if profile != "manager":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=f"Resource is forbidden",
        )
