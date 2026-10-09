from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class InternalEventEnvelope(BaseModel):
    """Versioned JSON envelope used only on Integration Platform Kafka topics."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: datetime
    correlation_id: str | None = None
    schema_version: Literal[1] = 1
    payload: dict[str, Any]

    def as_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json_bytes(cls, value: bytes) -> "InternalEventEnvelope":
        return cls.model_validate_json(value)


class DeliveryRouteSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    version: int


class DeliveryTargetSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    system_code: str
    url: str
    http_method: str
    connect_timeout_ms: int = Field(default=5000, ge=1)
    read_timeout_ms: int = Field(default=5000, ge=1)


class DeliveryContentSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["JSON", "XML"]
    content_type: str
    body: Any


class DeliveryRetrySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(ge=1)
    strategy: str
    initial_delay_seconds: int = Field(ge=0)
    max_delay_seconds: int = Field(ge=0)
    multiplier: int = Field(ge=1)
    retryable_errors: list[str]


class DeliveryRequestedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delivery_id: str
    message_id: str
    route_processing_result_id: str
    route: DeliveryRouteSnapshot
    target: DeliveryTargetSnapshot
    content: DeliveryContentSnapshot
    retry: DeliveryRetrySnapshot


class DeliveryCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delivery_id: str
    message_id: str
    route_processing_result_id: str
    target_system: str
    status: Literal["DELIVERED", "DEAD_LETTER"]
    attempt_count: int = Field(ge=0)
    completed_at: datetime


class DeliveryDeadLetteredPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delivery_id: str
    message_id: str
    route_processing_result_id: str
    target_system: str
    reason: str
    attempt_count: int = Field(ge=0)
    last_error: dict[str, str | None]


def utc_now() -> datetime:
    return datetime.now(UTC)


def envelope_json(envelope: InternalEventEnvelope) -> dict[str, Any]:
    """Return JSON-compatible values suitable for SQLAlchemy JSON columns."""

    return json.loads(envelope.model_dump_json())
