from fastapi import APIRouter
from app.dependencies import registry

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    return {"status": "healthy", "registry": registry.stats()}