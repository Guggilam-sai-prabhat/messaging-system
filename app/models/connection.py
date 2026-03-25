"""
Connection data shapes.

Extracted from connection_registry.py so the registry
imports the shape, and other modules (like websocket_manager)
can reference ConnectionInfo without depending on the registry.
"""

from dataclasses import dataclass
from fastapi import WebSocket


@dataclass
class ConnectionInfo:
    """Metadata about a single WebSocket connection."""
    websocket: WebSocket
    user_id: str
    device_id: str
    connected_at: float