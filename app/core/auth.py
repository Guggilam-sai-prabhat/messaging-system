"""
Authentication dependencies for FastAPI.

Two entry points, one validation path:

  HTTP routes:
    Authorization: Bearer <jwt-access-token>
    → get_current_user() decodes JWT, returns user_id

  WebSocket:
    ws://host/ws?token=<jwt-access-token>
    → authenticate_websocket() decodes JWT, returns UserClaims

Both go through decode_access_token() in core/security.py.
"""

from fastapi import Depends, HTTPException, Query, WebSocket
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.security import decode_access_token, UserClaims


_bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> str:
    """Extract user_id from a JWT Bearer token.

    Used by all protected HTTP routes:
        @router.post("/channels")
        async def create_channel(
            body: ...,
            user_id: str = Depends(get_current_user),
        ):

    Returns just the user_id string — routes overwhelmingly
    need only this. For full claims, use get_current_claims().
    """
    claims = decode_access_token(credentials.credentials)
    if claims is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return claims.user_id


async def get_current_claims(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> UserClaims:
    """Extract full UserClaims from a JWT Bearer token.

    Use when you need email, device_id, etc:
        @router.get("/me")
        async def whoami(claims: UserClaims = Depends(get_current_claims)):
            return {"userId": claims.user_id, "email": claims.email}
    """
    claims = decode_access_token(credentials.credentials)
    if claims is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return claims


async def authenticate_websocket(
    websocket: WebSocket,
    token: str = Query(...),
) -> UserClaims:
    """Authenticate a WebSocket connection via query parameter.

    WebSockets can't send custom headers from browsers, so the
    JWT access token comes as a query param:
      ws://host/ws?token=eyJhbGciOiJIUzI1NiIs...

    Usage:
        @router.websocket("/ws")
        async def ws_endpoint(
            websocket: WebSocket,
            claims: UserClaims = Depends(authenticate_websocket),
        ):
            await websocket.accept()
            ...

    On failure, closes the WebSocket with 4001 (custom close
    code in 4000-4999 range per RFC 6455).
    """
    claims = decode_access_token(token)
    if claims is None:
        await websocket.close(code=4001, reason="Invalid or expired token")
        # Raise so the handler doesn't continue
        raise HTTPException(status_code=401, detail="Invalid token")
    return claims