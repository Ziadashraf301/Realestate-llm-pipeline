"""
Web UI Presentation Router.
Serves the HTML client application and isolates UI rendering from REST APIs.
"""

from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

web_router = APIRouter(include_in_schema=False)

template_path = Path(__file__).resolve().parent.parent / "web" / "templates" / "ui.html"
login_template_path = Path(__file__).resolve().parent.parent / "web" / "templates" / "login.html"


@web_router.get("/", response_class=HTMLResponse)
async def serve_ui():
    """Serves the interactive Arabic real estate discovery UI."""
    if template_path.exists():
        return HTMLResponse(content=template_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Real Estate Intelligence System API is Active. Visit <a href='/docs'>/docs</a></h1>")


@web_router.get("/login", response_class=HTMLResponse)
async def serve_login():
    """Serves the authentication and user onboarding interface."""
    if login_template_path.exists():
        return HTMLResponse(content=login_template_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Login Page is unavailable</h1>")

