"""
Authentication — extracted into its own module.

This isolates the auth contract: token in → claims out (or None).
When you swap in real JWT validation, only this file changes.
The WebSocket manager and REST routes never touch token internals.
"""

MOCK_TOKENS = {
    "token-alice-1": {"user_id": "alice", "device_id": "alice-phone"},
    "token-alice-2": {"user_id": "alice", "device_id": "alice-laptop"},
    "token-bob-1":   {"user_id": "bob",   "device_id": "bob-phone"},
}


def authenticate_token(token: str) -> dict | None:
    """Validate a token and return user claims, or None if invalid."""
    return MOCK_TOKENS.get(token)