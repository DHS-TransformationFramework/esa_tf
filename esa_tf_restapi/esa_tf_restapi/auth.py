from typing import Optional

from fastapi import Depends
from pydantic import BaseModel


class User(BaseModel):
    username: str
    roles: Optional[list] = []


def get_user(username, roles=""):
    if not username:
        return None
    if roles:
        roles = roles.split(",")
    return User(username=username, roles=roles)
