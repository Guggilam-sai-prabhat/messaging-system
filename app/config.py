"""
Centralized configuration via environment variables.

Why a config module instead of scattered os.getenv() calls?
  1. Single source of truth — every setting is documented in one place
  2. Validation at startup — pydantic catches missing/malformed vars
     BEFORE your server starts accepting traffic
  3. Type safety — no more str-to-int bugs buried in handler code
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Messaging System"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000


    jwt_secret_key:  str = "your-256-bit-random-secret-here"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    # ── OAuth: Google ─────────────────────────────────────────
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = "http://localhost:8000/auth/google/callback"

    # ── OAuth: GitHub ─────────────────────────────────────────
    github_client_id: str = ""
    github_client_secret: str = ""
    github_redirect_uri: str = "http://localhost:8000/auth/github/callback"

    # Future: Redis URL for multi-process registry
    # redis_url: str = "redis://localhost:6379"
    kafka_bootstrap_servers: str = "localhost:9092"

    redis_url: str = "redis://:redis@localhost:6379/0"
    dedup_ttl_seconds: int = 300
    database_url: str = "postgresql://postgres:new_password@localhost:5432/messaging"
    # extra="ignore": .env is shared with ai_service/config.py (e.g.
    # NVIDIA_*), which reads its own vars via plain os.getenv rather than
    # this Settings model — those keys aren't fields here and shouldn't
    # fail startup just because another module's config lives in the same file.
    model_config = {"env_file": ".env", "extra": "ignore"}

    # ── MinIO / S3 ────────────────────────────────────────────
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "documents"
    minio_use_ssl: bool = False


settings = Settings()