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

    # Future: Redis URL for multi-process registry
    # redis_url: str = "redis://localhost:6379"

    model_config = {"env_file": ".env"}


settings = Settings()