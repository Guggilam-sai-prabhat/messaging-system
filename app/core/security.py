"""
Security primitives — JWT tokens and password hashing.

This module has ZERO knowledge of the database, FastAPI, or
HTTP. It's pure crypto operations:
  - Hash a password
  - Verify a password
  - Create a JWT
  - Decode a JWT
  - Hash a refresh token (for DB storage)

Why separate from the auth service?
  The auth service orchestrates DB lookups, token rotation,
  OAuth flows. This module is the toolkit it uses. You can
  test JWT creation/verification without touching a database.

Dependencies (add to requirements.txt):
  PyJWT>=2.8.0
  bcrypt>=4.1.0
  cryptography>=41.0.0   (needed by PyJWT for RS256 if you switch later)
"""

import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional

import jwt
import bcrypt

from app.config import settings


# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
# These come from your settings/env. Shown here with defaults.
#
# In app/core/config.py:
#   class Settings(BaseSettings):
#       jwt_secret_key: str
#       jwt_algorithm: str = "HS256"
#       access_token_expire_minutes: int = 15
#       refresh_token_expire_days: int = 7
#       model_config = SettingsConfigDict(env_file=".env")
#
# In .env:
#   JWT_SECRET_KEY=your-256-bit-random-secret-here
#
# Generate a secure secret:
#   python -c "import secrets; print(secrets.token_urlsafe(32))"

JWT_SECRET = settings.jwt_secret_key
JWT_ALGORITHM = settings.jwt_algorithm
ACCESS_TOKEN_EXPIRE = timedelta(minutes=settings.access_token_expire_minutes)
REFRESH_TOKEN_EXPIRE = timedelta(days=settings.refresh_token_expire_days)


# ─────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────

@dataclass
class UserClaims:
    """Validated identity extracted from an access token.

    Structured return so your IDE auto-completes fields and
    typos like claims["usr_id"] become impossible.
    """
    user_id: str
    email: str
    device_id: Optional[str] = None


@dataclass
class TokenPair:
    """Issued after login or token refresh."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 0  # seconds until access token expires


# ─────────────────────────────────────────────────────────────
# Password hashing
# ─────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """Hash a password with bcrypt.

    bcrypt is the right choice for password hashing because:
      - It's intentionally slow (~100ms per hash). Argon2 is
        theoretically better, but bcrypt has decades of
        battle-testing and universal library support.
      - It embeds the salt in the hash string, so you don't
        need a separate salt column.
      - The work factor (12 rounds by default) is tunable.

    Do NOT use SHA-256 or even PBKDF2 for passwords. They're
    designed to be fast, which is the opposite of what you
    want for password storage.
    """
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(
        password.encode("utf-8"), salt
    ).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    """Check a plaintext password against a bcrypt hash."""
    return bcrypt.checkpw(
        password.encode("utf-8"),
        password_hash.encode("utf-8"),
    )


# ─────────────────────────────────────────────────────────────
# JWT access tokens
# ─────────────────────────────────────────────────────────────

def create_access_token(
    user_id: str,
    email: str,
    device_id: Optional[str] = None,
) -> str:
    """Create a short-lived JWT access token.

    Claims:
      sub   — user_id (standard JWT subject claim)
      email — for display / convenience
      did   — device_id (custom, for multi-device tracking)
      iat   — issued at
      exp   — expiration (15 min from now)
      type  — "access" (so we can reject refresh tokens
              used as access tokens and vice versa)

    Why HS256 and not RS256?
      HS256 (symmetric) is fine when the same service that
      signs tokens also verifies them. RS256 (asymmetric) is
      for when a separate auth server signs and multiple
      services verify with a public key. You're a single
      service — HS256 is simpler and faster.

      If you later add a separate auth service or need
      third-party verification, switch to RS256 and rotate
      to a key pair.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "email": email,
        "did": device_id,
        "iat": now,
        "exp": now + ACCESS_TOKEN_EXPIRE,
        "type": "access",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> Optional[UserClaims]:
    """Decode and validate an access token.

    Returns UserClaims on success, None on any failure:
      - Expired → None (client should use refresh token)
      - Tampered → None
      - Wrong token type → None

    We never raise here — the caller (FastAPI dependency)
    decides what HTTP status to return.
    """
    try:
        payload = jwt.decode(
            token, JWT_SECRET, algorithms=[JWT_ALGORITHM]
        )

        # Reject refresh tokens used as access tokens
        if payload.get("type") != "access":
            return None

        return UserClaims(
            user_id=payload["sub"],
            email=payload["email"],
            device_id=payload.get("did"),
        )
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


# ─────────────────────────────────────────────────────────────
# Refresh tokens
# ─────────────────────────────────────────────────────────────

def generate_refresh_token() -> str:
    """Generate a cryptographically random refresh token.

    This is NOT a JWT. Refresh tokens are opaque strings —
    their validity is determined by DB lookup, not by
    decoding. This means:
      - They can be revoked instantly (delete the DB row)
      - They don't leak claims if stolen
      - Their expiry is enforced server-side, not client-side

    64 bytes = 512 bits of entropy. Overkill, but storage is
    cheap and brute-force is impossible.
    """
    return secrets.token_urlsafe(64)


def hash_refresh_token(token: str) -> str:
    """SHA-256 hash of a refresh token for DB storage.

    We store the hash, not the raw token, same principle as
    passwords. If the DB is compromised, the attacker gets
    hashes that can't be reversed into valid tokens.

    Unlike passwords, we use SHA-256 (fast) instead of bcrypt
    (slow) because refresh tokens are high-entropy random
    strings, not human-chosen passwords. Brute-force is
    infeasible regardless of hash speed.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────

def get_refresh_token_expiry() -> datetime:
    """When a newly issued refresh token should expire."""
    return datetime.now(timezone.utc) + REFRESH_TOKEN_EXPIRE


def get_access_token_expire_seconds() -> int:
    """Seconds until access token expires — for the API response."""
    return int(ACCESS_TOKEN_EXPIRE.total_seconds())