from typing import Any
from xml.etree import ElementTree

from integration_api.message_processing.contracts import ExternalEventEnvelope, OrderCreatedPayload
from integration_api.message_processing.ports import (
    IntegrationRoute,
    RouteTransformationPort,
    TargetPayload,
    TransformationPort,
)


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


class RouteTransformationProvider(RouteTransformationPort):
    """Selects an explicit M2 transformation for each static route."""

    def __init__(
        self,
        crm_transformation: TransformationPort | None = None,
        warehouse_transformation: OrderCreatedToWarehouseReservationTransformation | None = None,
    ) -> None:
        self._crm_transformation = crm_transformation or OrderCreatedToCrmTransformation()
        self._warehouse_transformation = warehouse_transformation or OrderCreatedToWarehouseReservationTransformation()

    def transform(self, route: IntegrationRoute, event: ExternalEventEnvelope) -> TargetPayload:
        if route.target_system == "crm":
            return TargetPayload("application/json", self._crm_transformation.transform(event))
        if route.target_system == "warehouse":
            return TargetPayload("application/xml; charset=utf-8", self._warehouse_transformation.transform(event))
        raise ValueError(f"No transformation is registered for route {route.key}")
