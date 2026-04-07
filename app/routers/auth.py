"""
Auth HTTP endpoints — registration, login, OAuth, token lifecycle.

Endpoints:
  POST   /auth/register         — email/password signup
  POST   /auth/login            — email/password login
  POST   /auth/refresh          — exchange refresh token for new pair
  POST   /auth/logout           — revoke refresh token (single device)
  POST   /auth/logout-all       — revoke all refresh tokens (all devices)
  GET    /auth/google            — redirect to Google OAuth
  GET    /auth/google/callback   — handle Google OAuth callback
  GET    /auth/github            — redirect to GitHub OAuth
  GET    /auth/github/callback   — handle GitHub OAuth callback
  GET    /auth/me                — get current user profile
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, EmailStr

from app.dependencies import auth_service
from app.core.auth import get_current_user, get_current_claims
from app.core.security import UserClaims
from app.services.auth_service import (
    AuthError,
    EmailTakenError,
    InvalidCredentialsError,
    AccountDisabledError,
    InvalidRefreshTokenError,
    TokenReuseDetectedError,
    OAuthError,
)

router = APIRouter(prefix="/auth", tags=["auth"])


# ── Error mapping ─────────────────────────────────────────────

ERROR_STATUS_MAP = {
    "EMAIL_TAKEN": 409,
    "INVALID_CREDENTIALS": 401,
    "ACCOUNT_DISABLED": 403,
    "INVALID_REFRESH_TOKEN": 401,
    "TOKEN_REUSE_DETECTED": 401,
    "OAUTH_ERROR": 502,
}


def _raise_http(err: AuthError) -> None:
    status = ERROR_STATUS_MAP.get(err.code, 400)
    raise HTTPException(
        status_code=status,
        detail={"error": err.code, "message": err.message},
    )


# ── Request models ────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Minimum 8 characters.",
    )
    display_name: str = Field(
        ...,
        min_length=1,
        max_length=256,
    )
    device_id: str | None = Field(
        None,
        max_length=256,
        description="Client-provided device identifier for session tracking.",
    )


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., max_length=128)
    device_id: str | None = Field(None, max_length=256)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(
        ...,
        alias="refreshToken",
        description="The refresh token from a previous login or refresh.",
    )


class LogoutRequest(BaseModel):
    refresh_token: str = Field(..., alias="refreshToken")


# ── Endpoints ─────────────────────────────────────────────────

@router.post("/register", status_code=201)
async def register(body: RegisterRequest, request: Request):
    """Register a new account with email/password.

    Returns the user profile + access/refresh token pair.
    The client should store the refresh token securely
    (HttpOnly cookie or secure device storage — never
    localStorage on web).
    """
    try:
        return await auth_service.register(
            email=body.email,
            password=body.password,
            display_name=body.display_name,
            device_id=body.device_id,
            user_agent=request.headers.get("user-agent"),
        )
    except AuthError as e:
        _raise_http(e)


@router.post("/login")
async def login(body: LoginRequest, request: Request):
    """Login with email/password. Returns token pair."""
    try:
        return await auth_service.login(
            email=body.email,
            password=body.password,
            device_id=body.device_id,
            user_agent=request.headers.get("user-agent"),
        )
    except AuthError as e:
        _raise_http(e)


@router.post("/refresh")
async def refresh_tokens(body: RefreshRequest, request: Request):
    """Exchange a refresh token for a new token pair.

    The old refresh token is revoked (rotation). The client
    must use the new refresh token for the next refresh.

    If this returns TOKEN_REUSE_DETECTED, all sessions for
    the user have been revoked — they must login again.
    """
    try:
        return await auth_service.refresh_tokens(
            raw_refresh_token=body.refresh_token,
            user_agent=request.headers.get("user-agent"),
        )
    except AuthError as e:
        _raise_http(e)


@router.post("/logout")
async def logout(body: LogoutRequest):
    """Revoke a specific refresh token (single device logout).

    The access token remains valid until it expires (15 min).
    For instant invalidation, the client should discard the
    access token locally.
    """
    try:
        return await auth_service.logout(
            raw_refresh_token=body.refresh_token,
        )
    except AuthError as e:
        _raise_http(e)


@router.post("/logout-all")
async def logout_all(user_id: str = Depends(get_current_user)):
    """Revoke all refresh tokens for the current user.

    Use when: password changed, account compromised, or user
    clicks "sign out everywhere".
    """
    try:
        return await auth_service.logout_all_devices(user_id)
    except AuthError as e:
        _raise_http(e)


# ── Google OAuth ──────────────────────────────────────────────

@router.get("/google")
async def google_login():
    """Redirect to Google's OAuth consent screen.

    The client navigates to this URL. Google authenticates
    the user and redirects them back to /auth/google/callback
    with an authorization code.
    """
    url = auth_service.get_google_auth_url()
    return RedirectResponse(url=url)


@router.get("/google/callback")
async def google_callback(
    code: str,
    request: Request,
    state: str | None = None,
):
    """Handle Google's OAuth callback.

    Google redirects here with ?code=xxx after the user
    consents. We exchange the code for tokens and
    create/find the user.

    In production, verify the `state` parameter matches
    what you sent in /auth/google to prevent CSRF.
    """
    try:
        return await auth_service.handle_google_callback(
            code=code,
            user_agent=request.headers.get("user-agent"),
        )
    except AuthError as e:
        _raise_http(e)


# ── GitHub OAuth ──────────────────────────────────────────────

@router.get("/github")
async def github_login():
    """Redirect to GitHub's OAuth consent screen."""
    url = auth_service.get_github_auth_url()
    return RedirectResponse(url=url)


@router.get("/github/callback")
async def github_callback(
    code: str,
    request: Request,
    state: str | None = None,
):
    """Handle GitHub's OAuth callback."""
    try:
        return await auth_service.handle_github_callback(
            code=code,
            user_agent=request.headers.get("user-agent"),
        )
    except AuthError as e:
        _raise_http(e)


# ── User profile ──────────────────────────────────────────────

@router.get("/me")
async def get_me(claims: UserClaims = Depends(get_current_claims)):
    """Get the current user's profile from the access token.

    This doesn't hit the DB — it reads from the JWT claims.
    For full profile with DB data, add a /users/me endpoint
    that fetches from the users table.
    """
    return {
        "userId": claims.user_id,
        "email": claims.email,
        "deviceId": claims.device_id,
    }