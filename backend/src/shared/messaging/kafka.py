from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol


logger = logging.getLogger(__name__)


class KafkaProducerPort(Protocol):
    async def send(self, topic: str, key: str, value: bytes) -> None: ...


class AioKafkaProducerAdapter:
    """Small adapter which keeps aiokafka out of application services."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            acks="all",
            enable_idempotence=False,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(self, topic: str, key: str, value: bytes) -> None:
        await self.start()
        assert self._producer is not None
        await self._producer.send_and_wait(topic, value=value, key=key.encode("utf-8"))


class KafkaConsumerWorker:
    """Manual-offset consumer: handler commit happens before the Kafka offset."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        handle: Callable[[bytes], Awaitable[None]],
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._handle = handle

    async def run(self, stopped: asyncio.Event) -> None:
        while not stopped.is_set():
            consumer = None
            try:
                from aiokafka import AIOKafkaConsumer
                from aiokafka.structs import OffsetAndMetadata

                consumer = AIOKafkaConsumer(
                    self._topic,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._group_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                await consumer.start()
                logger.info("kafka_consumer_started", extra={"topic": self._topic, "group_id": self._group_id})
                while not stopped.is_set():
                    batches = await consumer.getmany(timeout_ms=1000, max_records=20)
                    for partition, records in batches.items():
                        for record in records:
                            # A handler failure deliberately leaves the offset uncommitted.
                            await self._handle(record.value)
                            await consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "")})
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("kafka_consumer_failed", extra={"topic": self._topic, "group_id": self._group_id})
                await asyncio.sleep(2)
            finally:
                if consumer is not None:
                    await consumer.stop()
