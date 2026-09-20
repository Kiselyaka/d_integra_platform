from fastapi import FastAPI

from integration_api.configuration.settings import Settings
from shared.database import create_database_engine
from shared.health import build_health_router


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="Integration API", version="0.1.0")
    app.include_router(build_health_router(create_database_engine(settings.database_url)))
    return app


app = create_app()

