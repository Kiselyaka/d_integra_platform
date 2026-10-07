from pydantic import ValidationError

from integration_api.message_processing.contracts import (
    ExternalEventEnvelope,
    OrderCreatedPayload,
    WarehouseReservationFailedV1,
    WarehouseReservationSucceededV1,
)


class PydanticSourceValidator:
    """Temporary M1 validator; replaceable by a schema provider in a later milestone."""

    def validate(self, event: ExternalEventEnvelope) -> str | None:
        payload_model = {
            ("mini-shop", "order.created", 1): OrderCreatedPayload,
            ("warehouse-1c", "warehouse.reservation.succeeded", 1): WarehouseReservationSucceededV1,
            ("warehouse-1c", "warehouse.reservation.failed", 1): WarehouseReservationFailedV1,
        }.get((event.source_system, event.event_type, event.schema_version))
        if payload_model is None:
            return "Unsupported source event contract"
        try:
            payload_model.model_validate(event.payload)
        except ValidationError as error:
            return error.json()
        return None
