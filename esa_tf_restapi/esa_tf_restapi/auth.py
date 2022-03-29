from typing import Optional

from pydantic import BaseModel

DEFAULT_USER = "no_user"


class User(BaseModel):
    username: str
    roles: Optional[list] = []


def get_user(username, roles=""):
    username = DEFAULT_USER if not username else username
    roles = roles.split(",") if isinstance(roles, str) else []
    return User(username=username, roles=roles)
