import os
import random
from typing import Optional

from fastapi import Depends
from pydantic import BaseModel


class User(BaseModel):
    username: str
    roles: Optional[list] = []


def get_user(username, roles=""):
    env = os.environ
    if "TF_USERNAME_TEST" in env and "TF_ROLE_TEST" in env:
        usernames = env["TF_USERNAME_TEST"].split(",")
        roles = env["TF_ROLE_TEST"].split(",")
        return User(username=random.choice(usernames), roles=random.choices(roles, k=2))
    if not username:
        return None
    if roles:
        roles = roles.split(",")
    return User(username=username, roles=roles)
