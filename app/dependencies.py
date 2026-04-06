"""
Shared FastAPI dependencies — singleton instances.

Instantiation order:
  1. registry          (no deps)
  2. ws_manager        (needs registry)
  3. membership_svc    (needs registry)
  4. channel_svc       (needs membership_svc + DB session factory)

All are module-level singletons. FastAPI's Depends() can
reference them directly.
"""

from app.core.connection_registry import connection_registry as registry
from app.services.websocket_manager import WebSocketManager
from app.services.channel_membership_service import ChannelMembershipService
from app.services.channel_service import ChannelService
from app.db.database import database

ws_manager = WebSocketManager(registry)
membership_service = ChannelMembershipService(registry)
channel_service = ChannelService(
    membership=membership_service,
    session_factory=database.get_session,
)