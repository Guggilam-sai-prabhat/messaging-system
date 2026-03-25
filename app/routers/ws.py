from fastapi import APIRouter, WebSocket, Query
from app.dependencies import ws_manager, registry

router = APIRouter(tags=["websocket"])


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...),
):
    await ws_manager.handle_connection(websocket, token)


@router.get("/users/{user_id}/status")
async def get_user_status(user_id: str):
    connections = registry.get_user_connections(user_id)
    return {
        "user_id": user_id,
        "online": len(connections) > 0,
        "connections": [
            {"device_id": c.device_id, "connected_at": c.connected_at}
            for c in connections
        ],
    }