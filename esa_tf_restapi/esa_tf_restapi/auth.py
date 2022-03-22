import os
import random
from typing import Optional

from pydantic import BaseModel

DEFAULT_USER = "no_user"


class User(BaseModel):
    username: str
    roles: Optional[list] = []


def get_user(username, roles=""):
    env = os.environ
    if "TF_USERNAME_TEST" in env and "TF_ROLE_TEST" in env:
        usernames = env["TF_USERNAME_TEST"].split(",")
        roles = env["TF_ROLE_TEST"].split(",")
        return User(username=random.choice(usernames), roles=random.choices(roles, k=2))
    username = DEFAULT_USER if not username else username
    roles = roles.split(",") if isinstance(roles, str) else []
    return User(username=username, roles=roles)
