from sqlalchemy import func, select
from fastapi.testclient import TestClient

from integration_api.configuration.settings import Settings
from integration_api.main import create_app
from integration_api.message_processing.models import Base, Message
from integration_api.message_processing.warehouse_xml import WarehouseXmlParseError, parse_warehouse_event
from shared.database import create_database_engine, create_session_factory


def _warehouse_xml(
    event_type: str = "reservation.succeeded",
    event_id: str = "3c8830d1-3fe1-4d89-8da6-c9075e09bb60",
    payload: str | None = None,
) -> str:
    if payload is None:
        payload = (
            "<ReservationSucceeded><OrderNumber>451</OrderNumber><Status>SUCCEEDED</Status></ReservationSucceeded>"
            if event_type == "reservation.succeeded"
            else "<ReservationFailed><OrderNumber>451</OrderNumber><Status>FAILED</Status>"
            "<Code>OUT_OF_STOCK</Code><Message>Insufficient stock for SKU-001</Message></ReservationFailed>"
        )
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<WarehouseEvent>
  <EventId>{event_id}</EventId>
  <EventType>{event_type}</EventType>
  <OccurredAt>2026-10-07T18:00:00+03:00</OccurredAt>
  <CorrelationId>order-451</CorrelationId>
  <SchemaVersion>1</SchemaVersion>
  <Payload>{payload}</Payload>
</WarehouseEvent>"""


def _client_and_session_factory() -> tuple[TestClient, object]:
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(Settings(database_url="sqlite+pysqlite:///:memory:"), session_factory=session_factory)
    return TestClient(app), session_factory


def _post_warehouse_event(client: TestClient, xml: str) -> object:
    return client.post("/api/v1/events/warehouse", content=xml.encode("utf-8"), headers={"Content-Type": "application/xml"})


def test_parser_creates_the_generic_succeeded_envelope() -> None:
    event = parse_warehouse_event(_warehouse_xml().encode("utf-8"))

    assert event.model_dump(mode="json") == {
        "event_id": "3c8830d1-3fe1-4d89-8da6-c9075e09bb60",
        "event_type": "warehouse.reservation.succeeded",
        "source_system": "warehouse-1c",
        "occurred_at": "2026-10-07T18:00:00+03:00",
        "correlation_id": "order-451",
        "schema_version": 1,
        "payload": {"external_order_id": "451", "status": "SUCCEEDED"},
    }


def test_accepts_and_persists_a_succeeded_warehouse_event() -> None:
    client, session_factory = _client_and_session_factory()

    response = _post_warehouse_event(client, _warehouse_xml())

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    with session_factory() as session:  # type: ignore[operator]
        message = session.scalar(select(Message))
    assert message is not None
    assert message.source_system == "warehouse-1c"
    assert message.event_type == "warehouse.reservation.succeeded"
    assert message.status == "NO_ROUTE"
    assert message.payload == {"external_order_id": "451", "status": "SUCCEEDED"}


def test_accepts_and_persists_a_failed_warehouse_event() -> None:
    client, session_factory = _client_and_session_factory()

    response = _post_warehouse_event(client, _warehouse_xml("reservation.failed"))

    assert response.status_code == 202
    with session_factory() as session:  # type: ignore[operator]
        message = session.scalar(select(Message))
    assert message is not None
    assert message.event_type == "warehouse.reservation.failed"
    assert message.payload == {
        "external_order_id": "451",
        "status": "FAILED",
        "reason_code": "OUT_OF_STOCK",
        "message": "Insufficient stock for SKU-001",
    }


def test_rejects_malformed_xml() -> None:
    client, _ = _client_and_session_factory()

    response = _post_warehouse_event(client, "<WarehouseEvent>")

    assert response.status_code == 400
    assert response.json()["detail"] == "Malformed XML"


def test_rejects_missing_required_envelope_field() -> None:
    client, _ = _client_and_session_factory()
    xml = _warehouse_xml().replace("<EventId>3c8830d1-3fe1-4d89-8da6-c9075e09bb60</EventId>", "")

    response = _post_warehouse_event(client, xml)

    assert response.status_code == 400
    assert response.json()["detail"] == "Missing required element: EventId"


def test_rejects_an_unsupported_event_type() -> None:
    client, _ = _client_and_session_factory()

    response = _post_warehouse_event(client, _warehouse_xml("inventory.changed"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported EventType: inventory.changed"


def test_rejects_an_invalid_succeeded_payload() -> None:
    client, _ = _client_and_session_factory()
    xml = _warehouse_xml(payload="<ReservationSucceeded><OrderNumber>451</OrderNumber></ReservationSucceeded>")

    response = _post_warehouse_event(client, xml)

    assert response.status_code == 400
    assert response.json()["detail"] == "Missing required element: Status"


def test_rejects_an_invalid_failed_payload() -> None:
    client, _ = _client_and_session_factory()
    xml = _warehouse_xml("reservation.failed", payload="<ReservationFailed><OrderNumber>451</OrderNumber><Status>FAILED</Status><Code>OUT_OF_STOCK</Code></ReservationFailed>")

    response = _post_warehouse_event(client, xml)

    assert response.status_code == 400
    assert response.json()["detail"] == "Missing required element: Message"


def test_accepting_the_same_warehouse_event_twice_is_idempotent() -> None:
    client, session_factory = _client_and_session_factory()
    xml = _warehouse_xml()

    first = _post_warehouse_event(client, xml)
    second = _post_warehouse_event(client, xml)

    assert first.status_code == second.status_code == 202
    assert first.json()["status"] == "accepted"
    assert second.json()["status"] == "already_accepted"
    assert first.json()["message_id"] == second.json()["message_id"]
    with session_factory() as session:  # type: ignore[operator]
        assert session.scalar(select(func.count()).select_from(Message)) == 1


def test_parser_rejects_missing_payload_element() -> None:
    xml = _warehouse_xml().replace("<Payload><ReservationSucceeded><OrderNumber>451</OrderNumber><Status>SUCCEEDED</Status></ReservationSucceeded></Payload>", "")

    try:
        parse_warehouse_event(xml.encode("utf-8"))
    except WarehouseXmlParseError as error:
        assert str(error) == "Missing required element: Payload"
    else:
        raise AssertionError("Expected WarehouseXmlParseError")
