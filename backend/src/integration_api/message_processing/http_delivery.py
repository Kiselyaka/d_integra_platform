import logging
import json
import httpx

from delivery_service.delivery.models import Delivery, DeliveryFailure
from delivery_service.delivery.ports import DeliveryExecution, DeliveryExecutorPort
from integration_api.message_processing.ports import IntegrationRoute, TargetDeliveryPort, TargetPayload

logger = logging.getLogger(__name__)


class CrmDeliveryError(RuntimeError):
    pass


class WarehouseDeliveryError(RuntimeError):
    pass


class MiniShopDeliveryError(RuntimeError):
    pass


class DeliveryTransportError(RuntimeError):
    """Structured HTTP outcome consumed by the Delivery aggregate."""

    def __init__(self, failure: DeliveryFailure) -> None:
        super().__init__(failure.message)
        self.delivery_failure = failure


def target_url_for_route(route: IntegrationRoute) -> str:
    paths = {
        "crm": "/api/integration/orders",
        "warehouse": "/hs/reservations/",
        "mini-shop": "/api/integration/reservation-results",
    }
    try:
        return f"{route.target_base_url.rstrip('/')}{paths[route.target_system]}"
    except KeyError as error:
        raise RuntimeError(f"No target URL is configured for route {route.key}") from error


def target_http_method_for_route(route: IntegrationRoute) -> str:
    return "POST"


class HttpDeliveryExecutor(DeliveryExecutorPort):
    """Executes an immutable Delivery snapshot rather than a live route."""

    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    def execute(self, delivery: Delivery) -> DeliveryExecution:
        if delivery.http_method != "POST":
            raise DeliveryTransportError(
                DeliveryFailure(
                    category="CONTRACT_ERROR",
                    code="UNSUPPORTED_HTTP_METHOD",
                    message=f"Unsupported delivery method: {delivery.http_method}",
                    retryable=False,
                )
            )
        headers = {"Content-Type": delivery.payload_format, "Idempotency-Key": delivery.idempotency_key}
        kwargs: dict[str, object] = {"headers": headers, "timeout": self._timeout_seconds}
        try:
            if delivery.payload_format.startswith("application/json"):
                kwargs["json"] = json.loads(delivery.transformed_payload)
            else:
                kwargs["content"] = delivery.transformed_payload.encode("utf-8")
        except (TypeError, json.JSONDecodeError) as error:
            raise DeliveryTransportError(
                DeliveryFailure(
                    category="CONTRACT_ERROR",
                    code="INVALID_SNAPSHOT_PAYLOAD",
                    message=f"Stored Delivery payload cannot be sent: {error}",
                    retryable=False,
                )
            ) from error
        try:
            response = httpx.request(delivery.http_method, delivery.target_url, **kwargs)
        except httpx.TimeoutException as error:
            raise DeliveryTransportError(
                DeliveryFailure("TIMEOUT", "TIMEOUT", str(error), retryable=True)
            ) from error
        except httpx.RequestError as error:
            raise DeliveryTransportError(
                DeliveryFailure("CONNECTION_ERROR", type(error).__name__, str(error), retryable=True)
            ) from error

        if _is_successful_delivery_response(delivery.target_system, response.status_code):
            return DeliveryExecution(http_status=response.status_code)
        raise DeliveryTransportError(_classify_http_failure(response.status_code))


def _is_successful_delivery_response(target_system: str, status_code: int) -> bool:
    if target_system == "warehouse":
        return status_code == 202
    return 200 <= status_code < 300


def _classify_http_failure(status_code: int) -> DeliveryFailure:
    if status_code == 429:
        return DeliveryFailure("RATE_LIMITED", "HTTP_429", "Target returned HTTP 429", retryable=True, http_status=status_code)
    if 500 <= status_code < 600:
        return DeliveryFailure(
            "REMOTE_SERVER_ERROR", f"HTTP_{status_code}", f"Target returned HTTP {status_code}", retryable=True, http_status=status_code
        )
    return DeliveryFailure(
        "CONTRACT_ERROR", f"HTTP_{status_code}", f"Target returned HTTP {status_code}", retryable=False, http_status=status_code
    )


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
                headers={"Content-Type": payload.content_type, "Idempotency-Key": idempotency_key},
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
