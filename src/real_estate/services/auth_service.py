"""
Auth Service (JWT Token Issuance, Password Hashing, Role-Based Access Control).
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
import bcrypt
import jwt
from fastapi import HTTPException, status

from real_estate.core.settings import settings
from real_estate.core.logger import logger
from real_estate.repositories.base import BaseUserRepository
from real_estate.schemas.auth import UserRegister, UserLogin, UserRead, Token, TokenPayload


class AuthService:
    """Manages authentication, salted bcrypt hashing, and JWT authorization."""

    def __init__(self, user_repo: BaseUserRepository):
        self.user_repo = user_repo

    async def hash_password(self, password: str) -> str:
        salt = bcrypt.gensalt(rounds=12)
        hashed = await asyncio.to_thread(bcrypt.hashpw, password.encode("utf-8"), salt)
        return hashed.decode("utf-8")

    async def verify_password(self, plain: str, hashed: str) -> bool:
        return await asyncio.to_thread(bcrypt.checkpw, plain.encode("utf-8"), hashed.encode("utf-8"))

    async def register_user(self, user_in: UserRegister) -> Token:
        existing = await self.user_repo.get_by_username(user_in.username)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already registered"
            )

        hashed = await self.hash_password(user_in.password)
        created_user = await self.user_repo.create(user_in, hashed)
        return self._create_token_response(created_user)

    async def authenticate_user(self, login_in: UserLogin) -> Token:
        user_record = await self.user_repo.get_by_username(login_in.username)
        if not user_record or not await self.verify_password(login_in.password, user_record["hashed_password"]):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password",
                headers={"WWW-Authenticate": "Bearer"}
            )

        user_read = UserRead(
            id=user_record["id"],
            username=user_record["username"],
            email=user_record["email"],
            role=user_record["role"],
            is_active=user_record["is_active"],
            searches_remaining=user_record.get("searches_remaining", 50)
        )
        return self._create_token_response(user_read)

    def _create_token_response(self, user: UserRead) -> Token:
        expire = datetime.now(timezone.utc) + timedelta(hours=settings.JWT_ACCESS_TOKEN_EXPIRE_HOURS)
        payload = {
            "sub": user.id,
            "username": user.username,
            "role": user.role,
            "exp": int(expire.timestamp()),
            "iat": int(datetime.now(timezone.utc).timestamp()),
        }
        token_str = jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
        return Token(
            access_token=token_str,
            token_type="Bearer",
            expires_in_hours=settings.JWT_ACCESS_TOKEN_EXPIRE_HOURS,
            user=user
        )

    def decode_token(self, token: str) -> TokenPayload:
        try:
            data = jwt.decode(
                token,
                settings.JWT_SECRET_KEY,
                algorithms=[settings.JWT_ALGORITHM]
            )
            return TokenPayload(**data)
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"}
            )
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"}
            )

    async def get_user_profile(self, user_id: str) -> UserRead | None:
        """Retrieves user profile by ID through the repository layer."""
        return await self.user_repo.get_by_id(user_id)

