from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    pass


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (UniqueConstraint("source_system", "external_event_id", name="uq_messages_source_external_event"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    external_event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class RouteProcessingResult(Base):
    __tablename__ = "route_processing_results"
    __table_args__ = (
        UniqueConstraint("message_id", "route_key", "route_version", name="uq_route_processing_results_message_route"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    message_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    route_key: Mapped[str] = mapped_column(String(128), nullable=False)
    route_version: Mapped[int] = mapped_column(Integer, nullable=False)
    # Configuration references are logical history, not FKs across contexts.
    route_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    transformation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    transformation_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_validation_schema_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    source_validation_schema_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_validation_schema_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_validation_schema_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    target_validation_schema_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    target_validation_schema_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    validation_schema_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    validation_schema_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_category: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_details: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProcessingOutboxEvent(Base):
    """Processing-owned event persisted before it is offered to Kafka."""

    __tablename__ = "processing_outbox_events"
    __table_args__ = (UniqueConstraint("event_id", name="uq_processing_outbox_events_event_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(36), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str] = mapped_column(String(36), nullable=False)
    envelope: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    publish_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_publish_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    publish_lease_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    publish_lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProcessingInboxMessage(Base):
    __tablename__ = "processing_inbox_messages"
    __table_args__ = (UniqueConstraint("consumer_name", "event_id", name="uq_processing_inbox_consumer_event"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    consumer_name: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class ProcessingDeliveryCompletion(Base):
    """Terminal Delivery fact projected into the Processing-owned persistence."""

    __tablename__ = "processing_delivery_completions"
    __table_args__ = (
        UniqueConstraint("route_processing_result_id", name="uq_processing_completion_route_result"),
        UniqueConstraint("delivery_id", name="uq_processing_completion_delivery"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    message_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    route_processing_result_id: Mapped[str] = mapped_column(String(36), nullable=False)
    delivery_id: Mapped[str] = mapped_column(String(36), nullable=False)
    terminal_status: Mapped[str] = mapped_column(String(32), nullable=False)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
