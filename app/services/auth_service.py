"""
Auth Service — registration, login, OAuth, token lifecycle.

Handles:
  1. Email/password registration and login
  2. Google OAuth (authorization code flow)
  3. GitHub OAuth (authorization code flow)
  4. Token refresh with rotation
  5. Logout (revoke refresh tokens)

Security design decisions:

  Refresh token rotation:
    Every time a refresh token is used, we issue a new one and
    revoke the old one. All tokens in a chain share a family_id.
    If a revoked token is replayed (theft detection), we revoke
    the ENTIRE family — every token in that chain is dead. The
    legitimate user gets logged out and must re-authenticate,
    but the attacker is also locked out.

  Timing-safe comparisons:
    We use bcrypt.checkpw (constant-time) for passwords and
    SHA-256 hash comparison for refresh tokens. No timing
    side-channels.

  No token blacklist for access tokens:
    Access tokens are short-lived (15 min). If a user logs out,
    we revoke their refresh tokens. The access token expires
    naturally. If you need instant invalidation (e.g., user
    banned), add a Redis blacklist keyed on jti with TTL =
    access token remaining lifetime. Not needed for most apps.

Dependencies:
  httpx>=0.27.0  (for OAuth HTTP calls to Google/GitHub)
"""

import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy import select, and_, update
from sqlalchemy.exc import IntegrityError

from app.db.models import User, RefreshToken
from app.core.security import (
    hash_password,
    verify_password,
    create_access_token,
    generate_refresh_token,
    hash_refresh_token,
    get_refresh_token_expiry,
    get_access_token_expire_seconds,
    TokenPair,
)
from app.config import settings

logger = logging.getLogger("auth.service")


# ─────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────

class AuthError(Exception):
    def __init__(self, message: str, code: str):
        self.message = message
        self.code = code
        super().__init__(message)


class EmailTakenError(AuthError):
    def __init__(self):
        super().__init__("Email already registered", "EMAIL_TAKEN")


class InvalidCredentialsError(AuthError):
    def __init__(self):
        super().__init__("Invalid email or password", "INVALID_CREDENTIALS")


class AccountDisabledError(AuthError):
    def __init__(self):
        super().__init__("Account is disabled", "ACCOUNT_DISABLED")


class InvalidRefreshTokenError(AuthError):
    def __init__(self):
        super().__init__("Invalid or expired refresh token", "INVALID_REFRESH_TOKEN")


class TokenReuseDetectedError(AuthError):
    """A revoked token was replayed — possible theft."""
    def __init__(self):
        super().__init__(
            "Refresh token reuse detected. All sessions revoked.",
            "TOKEN_REUSE_DETECTED",
        )


class OAuthError(AuthError):
    def __init__(self, provider: str, detail: str):
        super().__init__(
            f"OAuth error ({provider}): {detail}",
            "OAUTH_ERROR",
        )


# ─────────────────────────────────────────────────────────────
# Service
# ─────────────────────────────────────────────────────────────

class AuthService:

    def __init__(self, session_factory):
        self._session_factory = session_factory

    # ─────────────────────────────────────────────────────────
    # Email/password registration
    # ─────────────────────────────────────────────────────────

    async def register(
        self,
        email: str,
        password: str,
        display_name: str,
        device_id: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict:
        """Register a new account with email/password.

        Returns tokens immediately — no email verification.
        Add email verification later if needed:
          1. Set is_active=False at creation
          2. Send verification email with a signed token
          3. Verify endpoint flips is_active=True
          4. Login rejects is_active=False accounts

        Password requirements:
          Enforced at the route/Pydantic level, not here.
          The service trusts that the route validated length
          and complexity. This keeps validation visible in
          the API schema (OpenAPI docs).
        """
        user_id = str(uuid.uuid4())
        pwd_hash = hash_password(password)

        async with self._session_factory() as session:
            user = User(
                user_id=user_id,
                email=email.lower().strip(),
                display_name=display_name.strip(),
                password_hash=pwd_hash,
                last_login_at=datetime.now(timezone.utc),
            )
            session.add(user)

            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                raise EmailTakenError()

        # Issue tokens
        tokens = await self._issue_tokens(
            user_id=user_id,
            email=email.lower().strip(),
            device_id=device_id,
            user_agent=user_agent,
        )

        logger.info(f"Registered: user={user_id} email={email}")

        return {
            "userId": user_id,
            "email": email.lower().strip(),
            "displayName": display_name.strip(),
            **self._tokens_to_dict(tokens),
        }

    # ─────────────────────────────────────────────────────────
    # Email/password login
    # ─────────────────────────────────────────────────────────

    async def login(
        self,
        email: str,
        password: str,
        device_id: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict:
        """Authenticate with email/password.

        Why one error for both "email not found" and "wrong password"?
          Separate errors let an attacker enumerate valid emails.
          "Invalid email or password" reveals nothing.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(User).where(User.email == email.lower().strip())
            )
            user = result.scalar_one_or_none()

            if user is None:
                raise InvalidCredentialsError()

            if not user.is_active:
                raise AccountDisabledError()

            if user.password_hash is None:
                # OAuth-only account — can't login with password
                raise InvalidCredentialsError()

            if not verify_password(password, user.password_hash):
                raise InvalidCredentialsError()

            # Update last login
            user.last_login_at = datetime.now(timezone.utc)
            await session.commit()

        tokens = await self._issue_tokens(
            user_id=user.user_id,
            email=user.email,
            device_id=device_id,
            user_agent=user_agent,
        )

        logger.info(f"Login: user={user.user_id} email={user.email}")

        return {
            "userId": user.user_id,
            "email": user.email,
            "displayName": user.display_name,
            **self._tokens_to_dict(tokens),
        }

    # ─────────────────────────────────────────────────────────
    # Token refresh
    # ─────────────────────────────────────────────────────────

    async def refresh_tokens(
        self,
        raw_refresh_token: str,
        device_id: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict:
        """Exchange a refresh token for a new token pair.

        Token rotation:
          1. Look up the token by its hash
          2. If not found → invalid
          3. If revoked → TOKEN REUSE DETECTED
             → Revoke entire family (all tokens in the chain)
             → This forces the legitimate user to re-login,
               but also locks out the attacker
          4. If expired → invalid
          5. Otherwise → revoke this token, issue new pair
             with same family_id
        """
        token_hash = hash_refresh_token(raw_refresh_token)

        async with self._session_factory() as session:
            result = await session.execute(
                select(RefreshToken).where(
                    RefreshToken.token_hash == token_hash
                )
            )
            stored = result.scalar_one_or_none()

            if stored is None:
                raise InvalidRefreshTokenError()

            # ── Reuse detection ──────────────────────────────
            if stored.is_revoked:
                logger.warning(
                    f"Token reuse detected! family={stored.family_id} "
                    f"user={stored.user_id}"
                )
                # Revoke entire token family
                await session.execute(
                    update(RefreshToken)
                    .where(RefreshToken.family_id == stored.family_id)
                    .values(is_revoked=True)
                )
                await session.commit()
                raise TokenReuseDetectedError()

            # ── Expiry check ─────────────────────────────────
            if stored.expires_at < datetime.now(timezone.utc):
                stored.is_revoked = True
                await session.commit()
                raise InvalidRefreshTokenError()

            # ── Fetch user ───────────────────────────────────
            user_result = await session.execute(
                select(User).where(User.user_id == stored.user_id)
            )
            user = user_result.scalar_one_or_none()

            if user is None or not user.is_active:
                stored.is_revoked = True
                await session.commit()
                raise AccountDisabledError()

            # ── Rotate: revoke old, issue new ────────────────
            stored.is_revoked = True
            stored.last_used_at = datetime.now(timezone.utc)
            await session.commit()

        # Issue new pair with same family_id
        tokens = await self._issue_tokens(
            user_id=user.user_id,
            email=user.email,
            device_id=device_id,
            user_agent=user_agent,
            family_id=stored.family_id,
        )

        logger.info(
            f"Token refreshed: user={user.user_id} "
            f"family={stored.family_id}"
        )

        return {
            "userId": user.user_id,
            "email": user.email,
            "displayName": user.display_name,
            **self._tokens_to_dict(tokens),
        }

    # ─────────────────────────────────────────────────────────
    # Logout
    # ─────────────────────────────────────────────────────────

    async def logout(
        self,
        raw_refresh_token: str,
    ) -> dict:
        """Revoke a specific refresh token (single device logout)."""
        token_hash = hash_refresh_token(raw_refresh_token)

        async with self._session_factory() as session:
            result = await session.execute(
                select(RefreshToken).where(
                    RefreshToken.token_hash == token_hash
                )
            )
            stored = result.scalar_one_or_none()

            if stored and not stored.is_revoked:
                stored.is_revoked = True
                await session.commit()

        return {"loggedOut": True}

    async def logout_all_devices(self, user_id: str) -> dict:
        """Revoke ALL refresh tokens for a user (all devices)."""
        async with self._session_factory() as session:
            await session.execute(
                update(RefreshToken)
                .where(
                    and_(
                        RefreshToken.user_id == user_id,
                        RefreshToken.is_revoked == False,  # noqa: E712
                    )
                )
                .values(is_revoked=True)
            )
            await session.commit()

        logger.info(f"Logout all devices: user={user_id}")
        return {"loggedOut": True, "allDevices": True}

    # ─────────────────────────────────────────────────────────
    # Google OAuth
    # ─────────────────────────────────────────────────────────

    def get_google_auth_url(self, state: Optional[str] = None) -> str:
        """Build the Google OAuth authorization URL.

        The client redirects the user here. Google authenticates
        them and redirects back to our callback with a code.

        state parameter: opaque string the client can use to
        prevent CSRF. Should be a random value stored in the
        user's session, verified when the callback arrives.
        """
        params = {
            "client_id": settings.google_client_id,
            "redirect_uri": settings.google_redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "access_type": "offline",
        }
        if state:
            params["state"] = state

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"https://accounts.google.com/o/oauth2/v2/auth?{query}"

    async def handle_google_callback(
        self,
        code: str,
        device_id: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict:
        """Exchange Google auth code for tokens, create/find user.

        Flow:
          1. Exchange code for Google access token
          2. Use Google access token to fetch user profile
          3. Find or create user in our DB
          4. Issue our own JWT token pair
        """
        # ── Exchange code for Google tokens ──────────────────
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code,
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "redirect_uri": settings.google_redirect_uri,
                    "grant_type": "authorization_code",
                },
            )
            if token_resp.status_code != 200:
                raise OAuthError("google", f"Token exchange failed: {token_resp.text}")
            google_tokens = token_resp.json()

            # ── Fetch user profile ───────────────────────────
            profile_resp = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {google_tokens['access_token']}"},
            )
            if profile_resp.status_code != 200:
                raise OAuthError("google", "Failed to fetch profile")
            profile = profile_resp.json()

        google_id = profile["id"]
        email = profile["email"].lower().strip()
        name = profile.get("name", email.split("@")[0])

        return await self._oauth_find_or_create(
            provider="google",
            provider_id=google_id,
            email=email,
            display_name=name,
            device_id=device_id,
            user_agent=user_agent,
        )

    # ─────────────────────────────────────────────────────────
    # GitHub OAuth
    # ─────────────────────────────────────────────────────────

    def get_github_auth_url(self, state: Optional[str] = None) -> str:
        """Build the GitHub OAuth authorization URL."""
        params = {
            "client_id": settings.github_client_id,
            "redirect_uri": settings.github_redirect_uri,
            "scope": "user:email",
        }
        if state:
            params["state"] = state

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"https://github.com/login/oauth/authorize?{query}"

    async def handle_github_callback(
        self,
        code: str,
        device_id: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict:
        """Exchange GitHub auth code for tokens, create/find user."""
        async with httpx.AsyncClient() as client:
            # ── Exchange code ────────────────────────────────
            token_resp = await client.post(
                "https://github.com/login/oauth/access_token",
                data={
                    "code": code,
                    "client_id": settings.github_client_id,
                    "client_secret": settings.github_client_secret,
                    "redirect_uri": settings.github_redirect_uri,
                },
                headers={"Accept": "application/json"},
            )
            if token_resp.status_code != 200:
                raise OAuthError("github", f"Token exchange failed: {token_resp.text}")
            github_tokens = token_resp.json()

            access_token = github_tokens.get("access_token")
            if not access_token:
                raise OAuthError("github", "No access token in response")

            # ── Fetch user profile ───────────────────────────
            headers = {"Authorization": f"Bearer {access_token}"}
            profile_resp = await client.get(
                "https://api.github.com/user",
                headers=headers,
            )
            if profile_resp.status_code != 200:
                raise OAuthError("github", "Failed to fetch profile")
            profile = profile_resp.json()

            # ── Fetch email (might be private) ───────────────
            # GitHub doesn't always include email in /user.
            # The /user/emails endpoint has the primary email.
            email = profile.get("email")
            if not email:
                emails_resp = await client.get(
                    "https://api.github.com/user/emails",
                    headers=headers,
                )
                if emails_resp.status_code == 200:
                    emails = emails_resp.json()
                    primary = next(
                        (e for e in emails if e.get("primary")),
                        emails[0] if emails else None,
                    )
                    if primary:
                        email = primary["email"]

            if not email:
                raise OAuthError("github", "Could not retrieve email")

        github_id = str(profile["id"])
        name = profile.get("name") or profile.get("login") or email.split("@")[0]

        return await self._oauth_find_or_create(
            provider="github",
            provider_id=github_id,
            email=email.lower().strip(),
            display_name=name,
            device_id=device_id,
            user_agent=user_agent,
        )

    # ─────────────────────────────────────────────────────────
    # Shared OAuth logic
    # ─────────────────────────────────────────────────────────

    async def _oauth_find_or_create(
        self,
        provider: str,
        provider_id: str,
        email: str,
        display_name: str,
        device_id: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict:
        """Find existing OAuth user or create a new one.

        Three cases:
          1. User exists with this OAuth provider+id → login
          2. User exists with this email (password account) →
             link the OAuth provider to the existing account
          3. New user → create account with OAuth provider

        Case 2 is account linking: if someone registered with
        alice@gmail.com via password, then clicks "Login with
        Google" using the same email, we link the Google identity
        to their existing account. They can now use either method.

        This is a deliberate UX choice. The alternative (reject
        and say "email already registered") forces users to
        remember which method they used. Most apps do linking.
        """
        async with self._session_factory() as session:
            # Case 1: existing OAuth link
            result = await session.execute(
                select(User).where(
                    and_(
                        User.oauth_provider == provider,
                        User.oauth_provider_id == provider_id,
                    )
                )
            )
            user = result.scalar_one_or_none()

            if user is not None:
                if not user.is_active:
                    raise AccountDisabledError()

                user.last_login_at = datetime.now(timezone.utc)
                await session.commit()

                tokens = await self._issue_tokens(
                    user_id=user.user_id,
                    email=user.email,
                    device_id=device_id,
                    user_agent=user_agent,
                )
                logger.info(
                    f"OAuth login: provider={provider} user={user.user_id}"
                )
                return {
                    "userId": user.user_id,
                    "email": user.email,
                    "displayName": user.display_name,
                    "isNewUser": False,
                    **self._tokens_to_dict(tokens),
                }

            # Case 2: existing email → link OAuth
            result = await session.execute(
                select(User).where(User.email == email)
            )
            user = result.scalar_one_or_none()

            if user is not None:
                if not user.is_active:
                    raise AccountDisabledError()

                user.oauth_provider = provider
                user.oauth_provider_id = provider_id
                user.last_login_at = datetime.now(timezone.utc)
                await session.commit()

                tokens = await self._issue_tokens(
                    user_id=user.user_id,
                    email=user.email,
                    device_id=device_id,
                    user_agent=user_agent,
                )
                logger.info(
                    f"OAuth linked: provider={provider} "
                    f"user={user.user_id} email={email}"
                )
                return {
                    "userId": user.user_id,
                    "email": user.email,
                    "displayName": user.display_name,
                    "isNewUser": False,
                    **self._tokens_to_dict(tokens),
                }

            # Case 3: new user
            user_id = str(uuid.uuid4())
            user = User(
                user_id=user_id,
                email=email,
                display_name=display_name,
                oauth_provider=provider,
                oauth_provider_id=provider_id,
                last_login_at=datetime.now(timezone.utc),
            )
            session.add(user)

            try:
                await session.commit()
            except IntegrityError:
                # Race condition: another request created this user
                await session.rollback()
                return await self._oauth_find_or_create(
                    provider, provider_id, email, display_name,
                    device_id, user_agent,
                )

        tokens = await self._issue_tokens(
            user_id=user_id,
            email=email,
            device_id=device_id,
            user_agent=user_agent,
        )
        logger.info(
            f"OAuth registered: provider={provider} "
            f"user={user_id} email={email}"
        )
        return {
            "userId": user_id,
            "email": email,
            "displayName": display_name,
            "isNewUser": True,
            **self._tokens_to_dict(tokens),
        }

    # ─────────────────────────────────────────────────────────
    # Internal: issue token pair
    # ─────────────────────────────────────────────────────────

    async def _issue_tokens(
        self,
        user_id: str,
        email: str,
        device_id: Optional[str] = None,
        user_agent: Optional[str] = None,
        family_id: Optional[str] = None,
    ) -> TokenPair:
        """Create an access + refresh token pair.

        Writes the refresh token hash to the DB for later
        validation and revocation.

        family_id: if refreshing an existing chain, pass the
        family_id so the new token belongs to the same family.
        If None (new login), generate a new family_id.
        """
        access_token = create_access_token(
            user_id=user_id,
            email=email,
            device_id=device_id,
        )

        raw_refresh = generate_refresh_token()
        token_hash = hash_refresh_token(raw_refresh)
        token_id = str(uuid.uuid4())

        if family_id is None:
            family_id = str(uuid.uuid4())

        async with self._session_factory() as session:
            rt = RefreshToken(
                token_id=token_id,
                user_id=user_id,
                token_hash=token_hash,
                device_id=device_id,
                user_agent=user_agent,
                expires_at=get_refresh_token_expiry(),
                family_id=family_id,
            )
            session.add(rt)
            await session.commit()

        return TokenPair(
            access_token=access_token,
            refresh_token=raw_refresh,
            expires_in=get_access_token_expire_seconds(),
        )

    def _tokens_to_dict(self, tokens: TokenPair) -> dict:
        return {
            "accessToken": tokens.access_token,
            "refreshToken": tokens.refresh_token,
            "tokenType": tokens.token_type,
            "expiresIn": tokens.expires_in,
        }