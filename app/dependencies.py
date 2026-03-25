"""
Shared FastAPI dependencies — singleton instances.

Why a separate file?
  - Routers import from here, not from each other
  - Avoids circular imports (router → core → router)
  - Single place to swap in Redis-backed registry later
"""

from app.core.connection_registry import ConnectionRegistry
from app.core.websocket_manager import WebSocketManager

registry = ConnectionRegistry()
ws_manager = WebSocketManager(registry)