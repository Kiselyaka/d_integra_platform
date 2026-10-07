from typing import Any

from integration_api.message_processing.contracts import ExternalEventEnvelope, OrderCreatedPayload
from integration_api.message_processing.ports import TransformationPort


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
