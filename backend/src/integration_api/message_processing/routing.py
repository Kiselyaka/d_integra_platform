from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.ports import IntegrationRoute, RouteProvider


class StaticCrmRouteProvider(RouteProvider):
    """M1 adapter; later replaced by routes from Configuration Context."""

    def __init__(self, crm_base_url: str) -> None:
        self._crm_route = IntegrationRoute("mini-shop-order-created-to-crm", 1, crm_base_url.rstrip("/"))

    def find_route(self, event: ExternalEventEnvelope) -> IntegrationRoute | None:
        if (event.source_system, event.event_type, event.schema_version) == ("mini-shop", "order.created", 1):
            return self._crm_route
        return None
