from fastapi import APIRouter, Response, status
from sqlalchemy.engine import Engine

from shared.database import database_is_ready


def build_health_router(engine: Engine) -> APIRouter:
    router = APIRouter(tags=["health"])

    @router.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/health/ready")
    def ready(response: Response) -> dict[str, str]:
        if database_is_ready(engine):
            return {"status": "ok", "database": "available"}
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unavailable", "database": "unavailable"}

    return router

