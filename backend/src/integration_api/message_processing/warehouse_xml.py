"""XML transport adapter for events emitted by the Warehouse 1C system."""

from __future__ import annotations

from typing import Any
from xml.etree import ElementTree

from pydantic import ValidationError

from integration_api.message_processing.contracts import ExternalEventEnvelope


class WarehouseXmlParseError(ValueError):
    """The XML body does not satisfy the Warehouse transport contract."""


def parse_warehouse_event(xml_body: bytes) -> ExternalEventEnvelope:
    """Convert a Warehouse 1C XML event into the Platform's generic envelope."""
    try:
        root = ElementTree.fromstring(xml_body)
    except ElementTree.ParseError as error:
        raise WarehouseXmlParseError("Malformed XML") from error

    if root.tag != "WarehouseEvent":
        raise WarehouseXmlParseError("Root element must be WarehouseEvent")

    warehouse_event_type = _required_text(root, "EventType")
    # M8 deliberately keeps this adapter at the technical boundary.  A
    # syntactically valid but contract-invalid XML body must still resolve to a
    # route and become SOURCE_VALIDATION_FAILED after XSD validation.
    event_type, payload = _parse_payload(warehouse_event_type, root.find("Payload"))
    envelope_data: dict[str, Any] = {
        "event_id": _required_text(root, "EventId"),
        "event_type": event_type,
        # M6 normalizes the former internal code ``warehouse-1c`` to the
        # persistent ExternalSystem.code used by route lookup. This does not
        # change the 1C XML wire contract.
        "source_system": "1c-warehouse",
        "occurred_at": _required_text(root, "OccurredAt"),
        "schema_version": _schema_version(root),
        "payload": payload,
    }
    correlation_id = _optional_text(root, "CorrelationId")
    if correlation_id is not None:
        envelope_data["correlation_id"] = correlation_id

    try:
        return ExternalEventEnvelope.model_validate(envelope_data)
    except ValidationError as error:
        raise WarehouseXmlParseError("Invalid Warehouse event envelope") from error


def _parse_payload(event_type: str, payload: ElementTree.Element | None) -> tuple[str, dict[str, str | None]]:
    if event_type == "reservation.succeeded":
        reservation = payload.find("ReservationSucceeded") if payload is not None else None
        return (
            "warehouse.reservation.succeeded",
            {
                "external_order_id": _optional_text(reservation, "OrderNumber") if reservation is not None else None,
                "status": _optional_text(reservation, "Status") if reservation is not None else None,
            },
        )
    if event_type == "reservation.failed":
        reservation = payload.find("ReservationFailed") if payload is not None else None
        return (
            "warehouse.reservation.failed",
            {
                "external_order_id": _optional_text(reservation, "OrderNumber") if reservation is not None else None,
                "status": _optional_text(reservation, "Status") if reservation is not None else None,
                "reason_code": _optional_text(reservation, "Code") if reservation is not None else None,
                "message": _optional_text(reservation, "Message") if reservation is not None else None,
            },
        )
    raise WarehouseXmlParseError(f"Unsupported EventType: {event_type}")


def _schema_version(root: ElementTree.Element) -> int:
    value = _required_text(root, "SchemaVersion")
    try:
        return int(value)
    except ValueError as error:
        raise WarehouseXmlParseError("SchemaVersion must be an integer") from error


def _required_text(parent: ElementTree.Element, element_name: str) -> str:
    value = _optional_text(parent, element_name)
    if value is None:
        raise WarehouseXmlParseError(f"Missing required element: {element_name}")
    return value


def _optional_text(parent: ElementTree.Element, element_name: str) -> str | None:
    element = parent.find(element_name)
    if element is None or element.text is None:
        return None
    value = element.text.strip()
    return value or None


def _required_child(parent: ElementTree.Element, element_name: str) -> ElementTree.Element:
    element = parent.find(element_name)
    if element is None:
        raise WarehouseXmlParseError(f"Missing required element: {element_name}")
    return element
