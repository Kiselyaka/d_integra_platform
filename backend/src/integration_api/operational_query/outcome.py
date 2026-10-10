"""Pure M9 read-side Message outcome calculation."""

from __future__ import annotations

from collections.abc import Iterable


TERMINAL_MESSAGE_STATUSES = {"COMPLETED", "ROUTING_FAILED", "NO_ROUTE", "VALIDATION_FAILED"}


def derive_outcome(message_status: str, route_statuses: Iterable[str], delivery_statuses: Iterable[str]) -> str | None:
    """Return a presentation outcome; never changes Message state."""

    routes = list(route_statuses)
    deliveries = list(delivery_statuses)
    if message_status not in TERMINAL_MESSAGE_STATUSES:
        return "IN_PROGRESS"
    if not routes or message_status in {"NO_ROUTE", "VALIDATION_FAILED"}:
        return "FAILED"
    successful_deliveries = sum(status == "DELIVERED" for status in deliveries)
    failed_branches = sum(status != "DELIVERY_REQUESTED" for status in routes) + sum(
        status == "DEAD_LETTER" for status in deliveries
    )
    if successful_deliveries and failed_branches:
        return "PARTIAL_SUCCESS"
    if successful_deliveries and not failed_branches and all(status == "DELIVERED" for status in deliveries):
        return "SUCCESS"
    return "FAILED"
