from typing import Any
from xml.etree import ElementTree

from integration_api.message_processing.contracts import (
    ExternalEventEnvelope,
    OrderCreatedPayload,
    WarehouseReservationFailedV1,
    WarehouseReservationSucceededV1,
)
from integration_api.message_processing.field_mapping import FieldMappingV1Engine, TransformationConfigurationError
from integration_api.message_processing.ports import (
    IntegrationRoute,
    RouteTransformationPort,
    TargetPayload,
    TransformationPort,
)
from integration_api.message_processing.serialization import serialize_target_document


class OrderCreatedToCrmTransformation(TransformationPort):
    """M1 canonical mapping from a validated Mini Shop order to CRM's contract."""

    def transform(self, event: ExternalEventEnvelope) -> dict[str, Any]:
        if (event.source_system, event.event_type, event.schema_version) != ("mini-shop", "order.created", 1):
            raise ValueError("No transformation is registered for this event")

        source = OrderCreatedPayload.model_validate(event.payload)
        return {
            "external_order_id": source.order_id,
            "client": {
                "external_customer_id": source.customer.customer_id,
                "name": source.customer.full_name,
                "email": source.customer.email,
                "phone": source.customer.phone,
            },
            "deal": {"amount": str(source.total_amount), "currency": source.currency},
            "delivery_address": source.shipping_address.model_dump(mode="json"),
            "positions": [
                {
                    "external_product_code": item.product_code,
                    "title": item.name,
                    "quantity": item.quantity,
                    "price": str(item.unit_price),
                }
                for item in source.items
            ],
        }


class OrderCreatedToWarehouseReservationTransformation:
    """M2 mapping from a Mini Shop order into the published Warehouse XML contract."""

    def transform(self, event: ExternalEventEnvelope) -> str:
        if (event.source_system, event.event_type, event.schema_version) != ("mini-shop", "order.created", 1):
            raise ValueError("No Warehouse reservation transformation is registered for this event")

        source = OrderCreatedPayload.model_validate(event.payload)
        root = ElementTree.Element("ReservationRequest")
        ElementTree.SubElement(root, "OrderNumber").text = source.order_id
        positions = ElementTree.SubElement(root, "Positions")
        for item in source.items:
            position = ElementTree.SubElement(positions, "Position")
            ElementTree.SubElement(position, "ItemCode").text = item.product_code
            ElementTree.SubElement(position, "Quantity").text = str(item.quantity)
        return ElementTree.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


class WarehouseReservationSucceededToMiniShopTransformation:
    def transform(self, event: ExternalEventEnvelope) -> dict[str, Any]:
        if (
            event.source_system not in {"1c-warehouse", "warehouse-1c"}
            or event.event_type != "warehouse.reservation.succeeded"
            or event.schema_version != 1
        ):
            raise ValueError("No Mini Shop succeeded transformation is registered for this event")
        source = WarehouseReservationSucceededV1.model_validate(event.payload)
        return {
            "order_id": source.external_order_id,
            "reservation_result": "succeeded",
            "warehouse_event_id": event.event_id,
        }


class WarehouseReservationFailedToMiniShopTransformation:
    def transform(self, event: ExternalEventEnvelope) -> dict[str, Any]:
        if (
            event.source_system not in {"1c-warehouse", "warehouse-1c"}
            or event.event_type != "warehouse.reservation.failed"
            or event.schema_version != 1
        ):
            raise ValueError("No Mini Shop failed transformation is registered for this event")
        source = WarehouseReservationFailedV1.model_validate(event.payload)
        return {
            "order_id": source.external_order_id,
            "reservation_result": "failed",
            "warehouse_event_id": event.event_id,
            "reason_code": source.reason_code,
            "message": source.message,
        }


class RouteTransformationProvider(RouteTransformationPort):
    """Engine registry; M7 production routes use FIELD_MAPPING_V1 only.

    BUILTIN remains solely for M1--M6 historical fixtures and rows that may
    need an explicit compatibility path during an upgrade.
    """

    def __init__(
        self,
        crm_transformation: TransformationPort | None = None,
        warehouse_transformation: OrderCreatedToWarehouseReservationTransformation | None = None,
        warehouse_succeeded_to_mini_shop: WarehouseReservationSucceededToMiniShopTransformation | None = None,
        warehouse_failed_to_mini_shop: WarehouseReservationFailedToMiniShopTransformation | None = None,
        field_mapping_engine: FieldMappingV1Engine | None = None,
    ) -> None:
        crm = crm_transformation or OrderCreatedToCrmTransformation()
        warehouse = warehouse_transformation or OrderCreatedToWarehouseReservationTransformation()
        succeeded = warehouse_succeeded_to_mini_shop or WarehouseReservationSucceededToMiniShopTransformation()
        failed = warehouse_failed_to_mini_shop or WarehouseReservationFailedToMiniShopTransformation()
        self._handlers = {
            "order-created-to-crm": ("application/json", crm.transform),
            "order-created-to-warehouse-reservation": ("application/xml; charset=utf-8", warehouse.transform),
            "warehouse-reservation-succeeded-to-mini-shop": ("application/json", succeeded.transform),
            "warehouse-reservation-failed-to-mini-shop": ("application/json", failed.transform),
        }
        self._field_mapping_engine = field_mapping_engine or FieldMappingV1Engine()

    def transform(self, route: IntegrationRoute, event: ExternalEventEnvelope) -> TargetPayload:
        if route.transformation_engine_type == "FIELD_MAPPING_V1":
            if route.transformation_definition is None:
                raise TransformationConfigurationError("FIELD_MAPPING_V1 route has no definition")
            canonical_document = self._field_mapping_engine.transform(
                route.transformation_definition,
                event.payload,
                {"event": {"event_id": event.event_id, "event_type": event.event_type, "source_system": event.source_system}},
            )
            return serialize_target_document(route.target_format, canonical_document)
        if route.transformation_engine_type not in {None, "BUILTIN"}:
            raise TransformationConfigurationError(f"Unsupported transformation engine: {route.transformation_engine_type}")
        try:
            content_type, handler = self._handlers[route.transformation_key or _legacy_transformation_key(route, event)]
        except KeyError as error:
            raise ValueError(f"No transformation is registered for route {route.key}") from error
        return TargetPayload(content_type, handler(event))


def _legacy_transformation_key(route: IntegrationRoute, event: ExternalEventEnvelope) -> str:
    """Temporary compatibility only for direct M1--M5 unit fixtures."""

    legacy = {
        "crm": "order-created-to-crm",
        "warehouse": "order-created-to-warehouse-reservation",
        "1c-warehouse": "order-created-to-warehouse-reservation",
    }
    if route.target_system == "mini-shop":
        if event.event_type == "warehouse.reservation.succeeded":
            return "warehouse-reservation-succeeded-to-mini-shop"
        if event.event_type == "warehouse.reservation.failed":
            return "warehouse-reservation-failed-to-mini-shop"
    try:
        return legacy[route.target_system]
    except KeyError as error:
        raise ValueError(f"No legacy transformation mapping for {route.key}") from error
