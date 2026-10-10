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
            ("1c-warehouse", "warehouse.reservation.succeeded", 1): WarehouseReservationSucceededV1,
            ("1c-warehouse", "warehouse.reservation.failed", 1): WarehouseReservationFailedV1,
            # Historical records from M3--M5 retain their original logical
            # source identifier and remain readable during the M6 transition.
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


class TechnicalEnvelopeValidator:
    """M8 intake adapter.

    FastAPI/Pydantic has already validated the envelope itself. Business
    payload contracts are deliberately evaluated later per route by
    ``PreprocessingPipeline`` so fan-out routes stay independent.
    """

    def validate(self, event: ExternalEventEnvelope) -> str | None:
        return None
