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


@dataclass(frozen=True)
class RetryPolicyConfiguration:
    """Immutable retry settings resolved before a Delivery is requested."""

    id: str
    max_attempts: int
    strategy: str
    initial_delay_seconds: int
    max_delay_seconds: int
    multiplier: int
    retryable_errors: tuple[str, ...]


@dataclass(frozen=True)
class ValidationSchemaConfiguration:
    """Immutable source or target contract resolved with a route version."""

    id: str
    key: str
    version: int
    payload_format: str
    schema_type: str
    definition_json: dict[str, Any] | None
    definition_text: str | None


@dataclass(frozen=True)
class IntegrationRoute:
    """Runtime configuration snapshot owned by the Configuration Context.

    The first four fields retain the old M2 construction shape for focused
    legacy tests. Production M6 routes are always supplied by PostgreSQL and
    include the remaining immutable configuration fields.
    """

    key: str
    version: int
    target_base_url: str
    target_system: str = "crm"
    id: str | None = None
    target_endpoint: str = ""
    http_method: str = "POST"
    source_format: str = "JSON"
    target_format: str = "JSON"
    transformation_id: str | None = None
    transformation_key: str | None = None
    transformation_version: int | None = None
    transformation_engine_type: str | None = None
    transformation_definition: dict[str, Any] | None = None
    # Legacy M6 fields remain populated from source_schema for historical
    # readers. New M8 code uses the explicit references below.
    validation_schema_key: str | None = None
    validation_schema_version: int | None = None
    source_validation_schema: ValidationSchemaConfiguration | None = None
    target_validation_schema: ValidationSchemaConfiguration | None = None
    retry_policy: RetryPolicyConfiguration | None = None
    connect_timeout_ms: int = 5000
    read_timeout_ms: int = 5000
    configuration_error: str | None = None
