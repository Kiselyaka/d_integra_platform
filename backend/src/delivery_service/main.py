import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from delivery_service.configuration import Settings
from delivery_service.delivery.application import DeliveryProcessor
from delivery_service.delivery.models import DeliveryOutboxEvent
from delivery_service.delivery.requested_consumer import DeliveryRequestedConsumer
from integration_api.message_processing.http_delivery import HttpDeliveryExecutor
from shared.database import create_database_engine, create_session_factory
from shared.health import build_health_router
from shared.messaging.kafka import AioKafkaProducerAdapter, KafkaConsumerWorker
from shared.messaging.outbox import OutboxPublisher


logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = create_database_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    processor = DeliveryProcessor(
        session_factory,
        HttpDeliveryExecutor(settings.delivery_timeout_seconds),
        None,
        settings.delivery_stale_processing_timeout_seconds,
    )
    producer = AioKafkaProducerAdapter(settings.kafka_bootstrap_servers)
    outbox_publisher = OutboxPublisher(session_factory, DeliveryOutboxEvent, producer)
    requested_consumer = DeliveryRequestedConsumer(session_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stopped = asyncio.Event()

        async def transport_loop() -> None:
            while not stopped.is_set():
                try:
                    processor.recover_stale()
                    processor.process_available()
                except Exception:
                    logger.exception("delivery_worker_poll_failed")
                await asyncio.sleep(settings.delivery_poll_interval_seconds)

        async def publish_loop() -> None:
            while not stopped.is_set():
                try:
                    await outbox_publisher.publish_available()
                except Exception:
                    logger.exception("delivery_outbox_poll_failed")
                await asyncio.sleep(settings.kafka_outbox_poll_interval_seconds)

        consumer = KafkaConsumerWorker(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic="integration.delivery.requested",
            group_id="delivery-workers",
            handle=requested_consumer.handle_raw,
        )
        tasks = [
            asyncio.create_task(transport_loop()),
            asyncio.create_task(publish_loop()),
            asyncio.create_task(consumer.run(stopped)),
        ]
        try:
            yield
        finally:
            stopped.set()
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await producer.stop()

    app = FastAPI(title="Delivery Service", version="0.1.0", lifespan=lifespan)
    app.include_router(build_health_router(engine))
    return app


app = create_app()
