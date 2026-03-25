from fastapi import APIRouter, HTTPException
from app.dependencies import registry

router = APIRouter(prefix="/channels", tags=["channels"])


@router.get("/{channel_id}")
async def get_channel_info(channel_id: str):
    members = registry.get_channel_members(channel_id)
    if not members:
        raise HTTPException(404, "Channel not found or empty")
    return {
        "channel_id": channel_id,
        "members": [
            {
                "user_id": uid,
                "online": registry.is_user_online(uid),
                "connection_count": len(registry.get_user_connections(uid)),
            }
            for uid in members
        ],
    }


@router.post("/{channel_id}/members/{user_id}")
async def add_member(channel_id: str, user_id: str):
    was_new = registry.join_channel(channel_id, user_id)
    return {"channel_id": channel_id, "user_id": user_id, "newly_added": was_new}


@router.delete("/{channel_id}/members/{user_id}")
async def remove_member(channel_id: str, user_id: str):
    removed = registry.leave_channel(channel_id, user_id)
    if not removed:
        raise HTTPException(404, "User not in channel")
    return {"removed": True}