import logging
import httpx

from integration_api.message_processing.ports import IntegrationRoute, TargetDeliveryPort, TargetPayload

logger = logging.getLogger(__name__)


class CrmDeliveryError(RuntimeError):
    pass


class WarehouseDeliveryError(RuntimeError):
    pass


class MiniShopDeliveryError(RuntimeError):
    pass


class CrmHttpTargetDeliveryAdapter(TargetDeliveryPort):
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    def deliver(self, route: IntegrationRoute, payload: TargetPayload, idempotency_key: str) -> None:
        if not isinstance(payload.body, dict):
            raise CrmDeliveryError("CRM delivery requires a JSON object payload")
        url = f"{route.target_base_url}/api/integration/orders"
        logger.info("crm_delivery_started", extra={"route_key": route.key})
        try:
            response = httpx.post(
                url,
                json=payload.body,
                headers={"Idempotency-Key": idempotency_key},
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("crm_delivery_failed", extra={"route_key": route.key, "error": str(exc)})
            raise CrmDeliveryError(str(exc)) from exc
        logger.info("crm_delivery_completed", extra={"route_key": route.key})


class WarehouseHttpTargetDeliveryAdapter(TargetDeliveryPort):
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    def deliver(self, route: IntegrationRoute, payload: TargetPayload, idempotency_key: str) -> None:
        if not isinstance(payload.body, str):
            raise WarehouseDeliveryError("Warehouse delivery requires an XML string payload")
        url = f"{route.target_base_url}/hs/reservations/"
        logger.info("warehouse_delivery_started", extra={"route_key": route.key})
        try:
            response = httpx.post(
                url,
                content=payload.body.encode("utf-8"),
                headers={"Content-Type": payload.content_type},
                timeout=self._timeout_seconds,
            )
        except httpx.HTTPError as exc:
            logger.warning("warehouse_delivery_failed", extra={"route_key": route.key, "error": str(exc)})
            raise WarehouseDeliveryError(str(exc)) from exc
        if response.status_code != 202:
            error = f"Warehouse returned HTTP {response.status_code}"
            logger.warning("warehouse_delivery_failed", extra={"route_key": route.key, "error": error})
            raise WarehouseDeliveryError(error)
        logger.info("warehouse_delivery_completed", extra={"route_key": route.key})


class MiniShopHttpTargetDeliveryAdapter(TargetDeliveryPort):
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    def deliver(self, route: IntegrationRoute, payload: TargetPayload, idempotency_key: str) -> None:
        if not isinstance(payload.body, dict):
            raise MiniShopDeliveryError("Mini Shop delivery requires a JSON object payload")
        url = f"{route.target_base_url}/api/integration/reservation-results"
        logger.info("mini_shop_delivery_started", extra={"route_key": route.key})
        try:
            response = httpx.post(
                url,
                json=payload.body,
                headers={"Idempotency-Key": idempotency_key},
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("mini_shop_delivery_failed", extra={"route_key": route.key, "error": str(exc)})
            raise MiniShopDeliveryError(str(exc)) from exc
        logger.info("mini_shop_delivery_completed", extra={"route_key": route.key})


class RouteTargetDeliveryAdapter(TargetDeliveryPort):
    """Dispatches a target payload to the dedicated HTTP adapter for its route."""

    def __init__(
        self, crm_delivery: TargetDeliveryPort, warehouse_delivery: TargetDeliveryPort, mini_shop_delivery: TargetDeliveryPort
    ) -> None:
        self._deliveries = {"crm": crm_delivery, "warehouse": warehouse_delivery, "mini-shop": mini_shop_delivery}

    def deliver(self, route: IntegrationRoute, payload: TargetPayload, idempotency_key: str) -> None:
        try:
            delivery = self._deliveries[route.target_system]
        except KeyError as error:
            raise RuntimeError(f"No delivery adapter is registered for route {route.key}") from error
        delivery.deliver(route, payload, idempotency_key)


# Backward-compatible name for the existing CRM-specific adapter.
HttpTargetDeliveryAdapter = CrmHttpTargetDeliveryAdapter
