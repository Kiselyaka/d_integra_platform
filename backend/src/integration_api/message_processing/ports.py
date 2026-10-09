from dataclasses import dataclass
from typing import Any, Protocol

from integration_api.message_processing.contracts import ExternalEventEnvelope


class SourceValidationPort(Protocol):
    def validate(self, event: ExternalEventEnvelope) -> str | None: ...


class RouteProvider(Protocol):
    def find_matching_routes(self, event: ExternalEventEnvelope) -> list["IntegrationRoute"]: ...


class TransformationPort(Protocol):
    def transform(self, event: ExternalEventEnvelope) -> dict[str, Any]: ...


@dataclass(frozen=True)
class TargetPayload:
    content_type: str
    body: dict[str, Any] | str


class RouteTransformationPort(Protocol):
    def transform(self, route: "IntegrationRoute", event: ExternalEventEnvelope) -> TargetPayload: ...


class TargetDeliveryPort(Protocol):
    def deliver(self, route: "IntegrationRoute", payload: TargetPayload, idempotency_key: str) -> None: ...


class IntegrationRoute:
    def __init__(self, key: str, version: int, target_base_url: str, target_system: str = "crm") -> None:
        self.key = key
        self.version = version
        self.target_base_url = target_base_url
        self.target_system = target_system
