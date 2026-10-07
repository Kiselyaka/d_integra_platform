from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from integration_api.message_processing.contracts import ExternalEventEnvelope


def test_external_event_envelope_accepts_the_milestone_one_contract() -> None:
    event = ExternalEventEnvelope(
        event_id="event-451",
        event_type="order.created",
        source_system="mini-shop",
        occurred_at=datetime(2026, 10, 4, 12, 0, tzinfo=UTC),
        correlation_id="order-451",
        schema_version=1,
        payload={"order_id": "451"},
    )

    assert event.event_id == "event-451"
    assert event.correlation_id == "order-451"


@pytest.mark.parametrize("field", ["event_id", "event_type", "source_system", "occurred_at", "schema_version", "payload"])
def test_external_event_envelope_requires_mandatory_fields(field: str) -> None:
    payload = {
        "event_id": "event-451",
        "event_type": "order.created",
        "source_system": "mini-shop",
        "occurred_at": "2026-10-04T12:00:00Z",
        "schema_version": 1,
        "payload": {},
    }
    del payload[field]

    with pytest.raises(ValidationError):
        ExternalEventEnvelope.model_validate(payload)


def test_external_event_envelope_allows_missing_correlation_id() -> None:
    event = ExternalEventEnvelope.model_validate(
        {
            "event_id": "event-451",
            "event_type": "order.created",
            "source_system": "mini-shop",
            "occurred_at": "2026-10-04T12:00:00Z",
            "schema_version": 1,
            "payload": {},
        }
    )

    assert event.correlation_id is None
