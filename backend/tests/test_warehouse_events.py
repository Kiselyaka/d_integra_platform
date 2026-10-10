from sqlalchemy import func, select

from integration_api.message_processing.models import Base, Message
from integration_api.message_processing.service import EventIngestionService
from integration_api.message_processing.validation import PydanticSourceValidator
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
  <EventId>{event_id}</EventId><EventType>{event_type}</EventType><OccurredAt>2026-10-07T18:00:00+03:00</OccurredAt>
  <CorrelationId>order-451</CorrelationId><SchemaVersion>1</SchemaVersion><Payload>{payload}</Payload>
</WarehouseEvent>"""


def _ingestion() -> tuple[object, EventIngestionService]:
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    return session_factory, EventIngestionService(session_factory, PydanticSourceValidator())


def test_parser_creates_the_generic_succeeded_envelope() -> None:
    event = parse_warehouse_event(_warehouse_xml().encode("utf-8"))
    assert event.model_dump(mode="json") == {
        "event_id": "3c8830d1-3fe1-4d89-8da6-c9075e09bb60", "event_type": "warehouse.reservation.succeeded",
            "source_system": "1c-warehouse", "occurred_at": "2026-10-07T18:00:00+03:00",
        "correlation_id": "order-451", "schema_version": 1,
        "payload": {"external_order_id": "451", "status": "SUCCEEDED"},
    }


def test_ingests_succeeded_and_failed_warehouse_events() -> None:
    session_factory, ingestion = _ingestion()
    succeeded = ingestion.ingest(parse_warehouse_event(_warehouse_xml().encode("utf-8")))
    failed = ingestion.ingest(parse_warehouse_event(_warehouse_xml("reservation.failed", event_id="event-failed").encode("utf-8")))
    with session_factory() as session:  # type: ignore[operator]
        messages = list(session.scalars(select(Message).order_by(Message.external_event_id)))
    assert succeeded.duplicate is False and failed.duplicate is False
    assert [(message.event_type, message.status) for message in messages] == [
        ("warehouse.reservation.succeeded", "VALIDATED"), ("warehouse.reservation.failed", "VALIDATED")
    ]


def test_same_warehouse_event_is_idempotent() -> None:
    session_factory, ingestion = _ingestion()
    event = parse_warehouse_event(_warehouse_xml().encode("utf-8"))
    first = ingestion.ingest(event)
    second = ingestion.ingest(event)
    assert first.message_id == second.message_id and second.duplicate is True
    with session_factory() as session:  # type: ignore[operator]
        assert session.scalar(select(func.count()).select_from(Message)) == 1


def test_parser_rejects_malformed_or_unsupported_xml() -> None:
    invalid_cases = [
        ("<WarehouseEvent>", "Malformed XML"),
        (_warehouse_xml().replace("<EventId>3c8830d1-3fe1-4d89-8da6-c9075e09bb60</EventId>", ""), "Missing required element: EventId"),
        (_warehouse_xml("inventory.changed"), "Unsupported EventType: inventory.changed"),
    ]
    for xml, expected_error in invalid_cases:
        try:
            parse_warehouse_event(xml.encode("utf-8"))
        except WarehouseXmlParseError as error:
            assert str(error) == expected_error
        else:
            raise AssertionError("Expected WarehouseXmlParseError")


def test_parser_keeps_contract_invalid_xml_for_route_level_xsd_validation() -> None:
    xml = _warehouse_xml().replace(
        "<Payload><ReservationSucceeded><OrderNumber>451</OrderNumber><Status>SUCCEEDED</Status></ReservationSucceeded></Payload>", ""
    )
    event = parse_warehouse_event(xml.encode("utf-8"))
    assert event.event_type == "warehouse.reservation.succeeded"
    assert event.payload == {"external_order_id": None, "status": None}
