"""
Shared FastAPI dependencies — singleton instances.

Instantiation order matters (each depends on the ones above):
  1. registry          (no deps)
  2. membership_svc    (needs registry)
  3. channel_svc       (needs membership_svc + DB)
  4. ws_manager        (needs registry + membership_svc + channel_svc)
  5. auth_svc          (needs DB)
"""

from app.core.connection_registry import connection_registry as registry
from app.services.websocket_manager import WebSocketManager
from app.services.channel_membership_service import ChannelMembershipService
from app.services.channel_service import ChannelService
from app.services.auth_service import AuthService
from app.db.database import database

membership_service = ChannelMembershipService(registry)

channel_service = ChannelService(
    membership=membership_service,
    session_factory=database.get_session,
)

ws_manager = WebSocketManager(
    registry=registry,
    membership=membership_service,
    channel_service=channel_service,
)

auth_service = AuthService(
    session_factory=database.get_session,
)