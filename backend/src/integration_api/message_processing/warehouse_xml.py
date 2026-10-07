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
    payload_element = root.find("Payload")
    if payload_element is None:
        raise WarehouseXmlParseError("Missing required element: Payload")

    event_type, payload = _parse_payload(warehouse_event_type, payload_element)
    envelope_data: dict[str, Any] = {
        "event_id": _required_text(root, "EventId"),
        "event_type": event_type,
        "source_system": "warehouse-1c",
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


def _parse_payload(event_type: str, payload: ElementTree.Element) -> tuple[str, dict[str, str]]:
    if event_type == "reservation.succeeded":
        reservation = _required_child(payload, "ReservationSucceeded")
        return (
            "warehouse.reservation.succeeded",
            {
                "external_order_id": _required_text(reservation, "OrderNumber"),
                "status": _required_text(reservation, "Status"),
            },
        )
    if event_type == "reservation.failed":
        reservation = _required_child(payload, "ReservationFailed")
        return (
            "warehouse.reservation.failed",
            {
                "external_order_id": _required_text(reservation, "OrderNumber"),
                "status": _required_text(reservation, "Status"),
                "reason_code": _required_text(reservation, "Code"),
                "message": _required_text(reservation, "Message"),
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
