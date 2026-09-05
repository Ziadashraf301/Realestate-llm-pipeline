"""
Pytest Fixtures and Global Test Configuration (Clean Architecture).
Provides mock test client, auth tokens for all RBAC roles, and isolated test doubles.
"""

from typing import Any
import pytest
from fastapi.testclient import TestClient

from real_estate.main import app
from real_estate.core.settings import settings
from real_estate.services.auth_service import AuthService
from real_estate.schemas.auth import UserRegister, UserRead
from real_estate.repositories.user_repository import RedisUserRepository
from real_estate.repositories.property_repository import RedisPropertyRepository
from real_estate.api.deps import get_user_repository, get_property_repository


class FakeRedisPipeline:
    def __init__(self, store, zsets):
        self.store = store
        self.zsets = zsets
        self.ops = []

    def set(self, key, value):
        self.ops.append(("set", key, value))

    def zadd(self, name, mapping):
        self.ops.append(("zadd", name, mapping))

    def delete(self, *keys):
        self.ops.append(("delete", keys))

    def zrem(self, name, *values):
        self.ops.append(("zrem", name, values))

    async def execute(self):
        res: list[Any] = []
        for op in self.ops:
            if op[0] == "set":
                self.store[op[1]] = op[2]
                res.append(True)
            elif op[0] == "zadd":
                if op[1] not in self.zsets:
                    self.zsets[op[1]] = {}
                self.zsets[op[1]].update(op[2])
                res.append(len(op[2]))
            elif op[0] == "delete":
                deleted_count = 0
                for k in op[1]:
                    if k in self.store:
                        del self.store[k]
                        deleted_count += 1
                res.append(deleted_count)
            elif op[0] == "zrem":
                rem_count = 0
                if op[1] in self.zsets:
                    for v in op[2]:
                        if v in self.zsets[op[1]]:
                            del self.zsets[op[1]][v]
                            rem_count += 1
                res.append(rem_count)
        return res

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeRedisClient:
    def __init__(self):
        self.store = {}
        self.zsets = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def mget(self, keys: list[str]):
        return [self.store.get(k) for k in keys]

    async def set(self, key: str, value: str, ex=None):
        self.store[key] = value

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)

    async def zadd(self, name: str, mapping: dict):
        if name not in self.zsets:
            self.zsets[name] = {}
        self.zsets[name].update(mapping)

    async def zrevrange(self, name: str, start: int, stop: int):
        if name not in self.zsets:
            return []
        sorted_items = sorted(self.zsets[name].items(), key=lambda x: x[1], reverse=True)
        keys = [item[0] for item in sorted_items]
        if stop == -1:
            return keys[start:]
        return keys[start: stop + 1]

    async def zrem(self, name: str, *values):
        if name not in self.zsets:
            return 0
        cnt = 0
        for v in values:
            if v in self.zsets[name]:
                del self.zsets[name][v]
                cnt += 1
        return cnt

    async def scan(self, cursor=0, match=None, count=100):
        prefix = match.replace("*", "") if match else ""
        keys = [k for k in self.store.keys() if k.startswith(prefix)]
        return 0, keys

    def pipeline(self, transaction=True):
        return FakeRedisPipeline(self.store, self.zsets)


@pytest.fixture(scope="session")
def fake_redis():
    return FakeRedisClient()


@pytest.fixture(scope="session")
def test_user_repo(fake_redis):
    return RedisUserRepository(client=fake_redis)


@pytest.fixture(scope="session")
def test_property_repo(fake_redis):
    return RedisPropertyRepository(client=fake_redis)


@pytest.fixture(scope="session")
def client(test_user_repo, test_property_repo):
    """FastAPI TestClient session fixture with overridden user and property repos."""
    app.dependency_overrides[get_user_repository] = lambda: test_user_repo
    app.dependency_overrides[get_property_repository] = lambda: test_property_repo
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture(scope="session")
def auth_service(test_user_repo):
    """Auth service instance backed by test_user_repo."""
    return AuthService(user_repo=test_user_repo)


@pytest.fixture
def admin_token(auth_service):
    """Generates a valid JWT token with 'admin' role."""
    admin_user = UserRead(
        id="test_admin_id",
        username="admin_test",
        email="admin@test.com",
        role="admin",
        is_active=True,
        searches_remaining=999
    )
    token_resp = auth_service._create_token_response(admin_user)
    return token_resp.access_token


@pytest.fixture
def agent_token(auth_service):
    """Generates a valid JWT token with 'agent' role."""
    agent_user = UserRead(
        id="test_agent_id",
        username="agent_test",
        email="agent@test.com",
        role="agent",
        is_active=True,
        searches_remaining=999
    )
    token_resp = auth_service._create_token_response(agent_user)
    return token_resp.access_token


@pytest.fixture
def user_token(auth_service):
    """Generates a valid JWT token with 'user' role."""
    normal_user = UserRead(
        id="test_user_id",
        username="user_test",
        email="user@test.com",
        role="user",
        is_active=True,
        searches_remaining=50
    )
    token_resp = auth_service._create_token_response(normal_user)
    return token_resp.access_token


@pytest.fixture
def admin_headers(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def agent_headers(agent_token):
    return {"Authorization": f"Bearer {agent_token}"}


@pytest.fixture
def user_headers(user_token):
    return {"Authorization": f"Bearer {user_token}"}
