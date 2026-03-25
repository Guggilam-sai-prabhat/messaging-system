"""
Models package — re-exports all data shapes for clean imports.

Usage:
    from app.models import ConnectionInfo, IncomingMessage, EnrichedMessage

Instead of:
    from app.models.connection import ConnectionInfo
    from app.models.message import IncomingMessage, EnrichedMessage

The re-export pattern keeps import lines short across the codebase
while still allowing each model file to stay focused.
"""

from app.models.connection import ConnectionInfo
from app.models.message import IncomingMessage, EnrichedMessage

__all__ = [
    "ConnectionInfo",
    "IncomingMessage",
    "EnrichedMessage",
]