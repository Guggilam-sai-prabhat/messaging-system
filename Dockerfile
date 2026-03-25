# Multi-stage build: uv resolves deps in builder, slim image runs the app
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache

# ── Runtime stage ─────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app
COPY --from=builder /app /app
COPY app ./app

EXPOSE 8000

# Use the venv's fastapi CLI directly — no uv needed at runtime
CMD ["/app/.venv/bin/fastapi", "run", "app/main.py", "--port", "8000", "--host", "0.0.0.0"]