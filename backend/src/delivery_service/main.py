import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from delivery_service.configuration import Settings
from delivery_service.delivery.application import DeliveryProcessor
from integration_api.message_processing.delivery_completion import MessageDeliveryCompletionService
from integration_api.message_processing.http_delivery import HttpDeliveryExecutor
from shared.database import create_database_engine, create_session_factory
from shared.health import build_health_router


logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = create_database_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    processor = DeliveryProcessor(
        session_factory,
        HttpDeliveryExecutor(settings.delivery_timeout_seconds),
        MessageDeliveryCompletionService(session_factory),
        settings.delivery_stale_processing_timeout_seconds,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        running = True

        async def poll() -> None:
            while running:
                try:
                    processor.recover_stale()
                    processor.process_available()
                except Exception:
                    # The API process applies migrations at startup.  Retrying
                    # the next poll prevents a short startup race from killing
                    # the M4 worker permanently.
                    logger.exception("delivery_worker_poll_failed")
                await asyncio.sleep(settings.delivery_poll_interval_seconds)

        task = asyncio.create_task(poll())
        try:
            yield
        finally:
            running = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="Delivery Service", version="0.1.0", lifespan=lifespan)
    app.include_router(build_health_router(engine))
    return app


app = create_app()
