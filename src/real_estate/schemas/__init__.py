"""Domain Layer Schemas (Pydantic v2)."""

from real_estate.schemas.property import PropertyBase, PropertyCreate, PropertyUpdate, PropertyRead
from real_estate.schemas.rag import RAGQueryRequest, RAGResponse
from real_estate.schemas.auth import UserRegister, UserLogin, UserRead, Token, TokenPayload
from real_estate.schemas.intent import ExtractedQueryIntent

__all__ = [
    "PropertyBase",
    "PropertyCreate",
    "PropertyUpdate",
    "PropertyRead",
    "RAGQueryRequest",
    "RAGResponse",
    "UserRegister",
    "UserLogin",
    "UserRead",
    "Token",
    "TokenPayload",
    "ExtractedQueryIntent",
]

