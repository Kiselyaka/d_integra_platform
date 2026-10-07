from typing import Any, Protocol

from integration_api.message_processing.contracts import ExternalEventEnvelope


class SourceValidationPort(Protocol):
    def validate(self, event: ExternalEventEnvelope) -> str | None: ...


class RouteProvider(Protocol):
    def find_route(self, event: ExternalEventEnvelope) -> "IntegrationRoute | None": ...


class TransformationPort(Protocol):
    def transform(self, event: ExternalEventEnvelope) -> dict[str, Any]: ...


class TargetDeliveryPort(Protocol):
    def deliver(self, route: "IntegrationRoute", payload: dict[str, Any], idempotency_key: str) -> None: ...


class IntegrationRoute:
    def __init__(self, key: str, version: int, target_base_url: str) -> None:
        self.key = key
        self.version = version
        self.target_base_url = target_base_url
