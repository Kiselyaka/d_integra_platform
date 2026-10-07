# Mini Shop `order.created` v1

This source-specific payload is sent inside the [External Event Envelope v1](external-event-envelope-v1.md).

The route identity is fixed for Milestone 1:

```text
source_system = mini-shop
event_type = order.created
schema_version = 1
```

```json
{
  "order_id": "8fd1afc9-4d3b-4a77-9b07-1a95cf9be93c",
  "customer": {
    "customer_id": "customer-42",
    "full_name": "Ada Lovelace",
    "email": "ada@example.test",
    "phone": "+79990000000"
  },
  "shipping_address": {
    "country": "RU",
    "city": "Moscow",
    "postal_code": "101000",
    "street": "Tverskaya",
    "building": "1",
    "apartment": "2"
  },
  "items": [
    {
      "product_code": "SKU-001",
      "name": "Example Product",
      "quantity": 2,
      "unit_price": "10.50"
    }
  ],
  "currency": "EUR",
  "total_amount": "21.00"
}
```

`order_id`, customer and address fields, at least one item, and a three-letter currency code are required. Amounts use JSON decimal strings so they preserve monetary precision. The Platform validates this payload at ingress but the Mini Shop owns its own implementation of this public JSON contract.
