"""
Authentication & Authorization Endpoints (JWT + Bcrypt).
"""

from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, status
from real_estate.api.deps import get_auth_service, get_current_user
from real_estate.services.auth_service import AuthService
from real_estate.schemas.auth import UserRegister, UserLogin, UserRead, Token, TokenPayload

router = APIRouter(prefix="/auth", tags=["Authentication & Identity"])


@router.post("/register", response_model=Token, status_code=status.HTTP_201_CREATED, summary="Create User Account")
async def register(
    user_in: UserRegister,
    auth_service: Annotated[AuthService, Depends(get_auth_service)]
):
    return await auth_service.register_user(user_in)


@router.post("/login", response_model=Token, summary="Authenticate & Issue JWT Token")
async def login(
    login_in: UserLogin,
    auth_service: Annotated[AuthService, Depends(get_auth_service)]
):
    return await auth_service.authenticate_user(login_in)


@router.get("/me", response_model=UserRead, summary="Get Current Authenticated User Profile")
async def get_me(
    current_user: Annotated[TokenPayload, Depends(get_current_user)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)]
):
    user = await auth_service.get_user_profile(current_user.sub)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user
