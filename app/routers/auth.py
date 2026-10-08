import hmac

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from app.config import settings
from app.security import SESSION_COOKIE, make_session_token, verify_session_token

router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginRequest(BaseModel):
    password: str


@router.post("/login")
def login(payload: LoginRequest, response: Response):
    if not settings.ui_password:
        raise HTTPException(status_code=503, detail="UI login is disabled: set UI_PASSWORD in .env")
    if not hmac.compare_digest(payload.password.encode(), settings.ui_password.encode()):
        raise HTTPException(status_code=401, detail="Wrong password")
    response.set_cookie(
        SESSION_COOKIE, make_session_token(),
        max_age=settings.session_ttl_seconds, httponly=True, samesite="strict",
    )
    return {"ok": True}


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(SESSION_COOKIE)
    return {"ok": True}


@router.get("/me")
def me(request: Request):
    return {"authenticated": verify_session_token(request.cookies.get(SESSION_COOKIE))}
