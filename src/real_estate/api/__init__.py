"""API and Routing Module."""

from real_estate.api.v1.api import api_v1_router
from real_estate.api.web_router import web_router

__all__ = ["api_v1_router", "web_router"]
