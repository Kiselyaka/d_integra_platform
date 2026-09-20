from fastapi import FastAPI

from delivery_service.configuration import Settings
from shared.database import create_database_engine
from shared.health import build_health_router


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="Delivery Service", version="0.1.0")
    app.include_router(build_health_router(create_database_engine(settings.database_url)))
    return app


app = create_app()

