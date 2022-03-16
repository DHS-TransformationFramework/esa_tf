import os
from typing import Optional

from fastapi import Depends
from pydantic import BaseModel

DEFAULT_USER = "no_user"


class User(BaseModel):
    username: str
    roles: Optional[list] = []


def get_user(username, roles=""):
    env = os.environ
    if "TF_USERNAME_TEST" in env and "TF_ROLE_TEST" in env:
        return User(username=env["TF_USERNAME_TEST"], roles=[env["TF_ROLE_TEST"]])
    username = DEFAULT_USER if not username else username
    roles = roles.split(",") if isinstance(roles, str) else []
    return User(username=username, roles=roles)
