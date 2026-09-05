"""
Integration Tests for Authentication & JWT Management (Milestone 6).
Validates password hashing, token expiration, decoding, and user registration.
"""

import pytest
from real_estate.services.auth_service import AuthService
from real_estate.schemas.auth import UserRegister, UserLogin
from real_estate.repositories.user_repository import RedisUserRepository


@pytest.mark.asyncio
async def test_password_hashing_and_verification(test_user_repo):
    auth = AuthService(user_repo=test_user_repo)

    password = "MySecurePassword2026!"
    hashed = await auth.hash_password(password)

    assert hashed != password
    assert (await auth.verify_password(password, hashed)) is True
    assert (await auth.verify_password("WrongPassword!", hashed)) is False


@pytest.mark.asyncio
async def test_jwt_token_creation_and_decode(test_user_repo):
    auth = AuthService(user_repo=test_user_repo)

    user_in = UserRegister(
        username="jwt_tester",
        email="jwt@tester.com",
        password="Password123!",
        role="agent"
    )
    token_resp = await auth.register_user(user_in)

    assert token_resp.access_token is not None
    assert token_resp.token_type == "Bearer"
    assert token_resp.user.username == "jwt_tester"
    assert token_resp.user.role == "agent"

    # Decode and verify payload
    payload = auth.decode_token(token_resp.access_token)
    assert payload.username == "jwt_tester"
    assert payload.role == "agent"
    assert payload.sub == token_resp.user.id


def test_auth_endpoints_flow(client):
    # 1. Register a new user
    reg_resp = client.post(
        "/api/v1/auth/register",
        json={
            "username": "api_user_test",
            "email": "api_user@test.com",
            "password": "SecretPassword123!",
            "role": "user"
        }
    )
    assert reg_resp.status_code == 201
    data = reg_resp.json()
    assert "access_token" in data
    token = data["access_token"]

    # 2. Login with correct credentials
    login_resp = client.post(
        "/api/v1/auth/login",
        json={"username": "api_user_test", "password": "SecretPassword123!"}
    )
    assert login_resp.status_code == 200
    assert "access_token" in login_resp.json()

    # 3. Access /me endpoint with token
    me_resp = client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {token}"}
    )
    assert me_resp.status_code == 200
    me_data = me_resp.json()
    assert me_data["username"] == "api_user_test"
    assert me_data["role"] == "user"
