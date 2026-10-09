from datetime import UTC, datetime

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.routing import StaticRouteProvider


def test_order_created_matches_crm_and_warehouse_routes() -> None:
    provider = StaticRouteProvider("http://crm:8000", "http://host.docker.internal:8081/WarehouseDemo_Base")
    event = ExternalEventEnvelope(
        event_id="event-001",
        source_system="mini-shop",
        event_type="order.created",
        occurred_at=datetime(2026, 10, 9, tzinfo=UTC),
        schema_version=1,
        payload={},
    )

    routes = provider.find_matching_routes(event)

    assert [(route.key, route.version, route.target_system) for route in routes] == [
        ("mini-shop-order-created-to-crm", 1, "crm"),
        ("mini-shop-order-created-to-warehouse", 1, "warehouse"),
    ]


def test_warehouse_callback_has_no_matched_route() -> None:
    provider = StaticRouteProvider("http://crm:8000", "http://host.docker.internal:8081/WarehouseDemo_Base")
    event = ExternalEventEnvelope(
        event_id="event-002",
        source_system="warehouse-1c",
        event_type="warehouse.reservation.succeeded",
        occurred_at=datetime(2026, 10, 9, tzinfo=UTC),
        schema_version=1,
        payload={},
    )

    assert provider.find_matching_routes(event) == []
