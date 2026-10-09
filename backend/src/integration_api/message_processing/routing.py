from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.ports import IntegrationRoute, RouteProvider


class StaticRouteProvider(RouteProvider):
    """M2 static routes; later replaced by routes from Configuration Context."""

    def __init__(self, crm_base_url: str, warehouse_base_url: str, mini_shop_base_url: str = "http://mini-shop:8002") -> None:
        self._order_created_routes = [
            IntegrationRoute("mini-shop-order-created-to-crm", 1, crm_base_url.rstrip("/"), "crm"),
            IntegrationRoute("mini-shop-order-created-to-warehouse", 1, warehouse_base_url.rstrip("/"), "warehouse"),
        ]
        self._warehouse_callback_routes = {
            "warehouse.reservation.succeeded": IntegrationRoute(
                "warehouse-reservation-succeeded-to-mini-shop", 1, mini_shop_base_url.rstrip("/"), "mini-shop"
            ),
            "warehouse.reservation.failed": IntegrationRoute(
                "warehouse-reservation-failed-to-mini-shop", 1, mini_shop_base_url.rstrip("/"), "mini-shop"
            ),
        }

    def find_matching_routes(self, event: ExternalEventEnvelope) -> list[IntegrationRoute]:
        if (event.source_system, event.event_type, event.schema_version) == ("mini-shop", "order.created", 1):
            return list(self._order_created_routes)
        if event.source_system == "warehouse-1c" and event.schema_version == 1:
            route = self._warehouse_callback_routes.get(event.event_type)
            return [route] if route is not None else []
        return []
