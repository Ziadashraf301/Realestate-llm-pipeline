"""
Authentication and User Domain Schemas (Pydantic v2).
"""

from typing import Literal
from pydantic import BaseModel, EmailStr, Field


class UserRegister(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=6, max_length=100)
    role: Literal["user", "agent", "admin"] = "user"


class UserLogin(BaseModel):
    username: str
    password: str


class UserRead(BaseModel):
    id: str
    username: str
    email: EmailStr
    role: str
    is_active: bool = True
    searches_remaining: int = 50


class Token(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in_hours: int
    user: UserRead


class TokenPayload(BaseModel):
    sub: str = Field(..., description="User ID")
    username: str
    role: str
    exp: int
