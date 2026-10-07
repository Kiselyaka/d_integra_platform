import logging
from typing import Any

import httpx

from integration_api.message_processing.ports import IntegrationRoute, TargetDeliveryPort

logger = logging.getLogger(__name__)


class CrmDeliveryError(RuntimeError):
    pass


class HttpTargetDeliveryAdapter(TargetDeliveryPort):
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    def deliver(self, route: IntegrationRoute, payload: dict[str, Any], idempotency_key: str) -> None:
        url = f"{route.target_base_url}/api/integration/orders"
        logger.info("crm_delivery_started", extra={"route_key": route.key})
        try:
            response = httpx.post(
                url,
                json=payload,
                headers={"Idempotency-Key": idempotency_key},
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("crm_delivery_failed", extra={"route_key": route.key, "error": str(exc)})
            raise CrmDeliveryError(str(exc)) from exc
        logger.info("crm_delivery_completed", extra={"route_key": route.key})
