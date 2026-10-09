# Warehouse ReservationRequest v1

## Scope

This is the outbound contract used by Integration Platform in Milestone 2 to submit a Mini Shop order to the published Warehouse 1С HTTP service.

```text
Integration Platform (Docker)
  → http://host.docker.internal:8081/WarehouseDemo_Base/hs/reservations/
  → Apache
  → Warehouse 1С
```

The address is supplied through `WAREHOUSE_BASE_URL`; it is not part of the application-domain mapping.

## Request

- Method: `POST`
- Content-Type: `application/xml; charset=utf-8`
- Expected transport success: `202 Accepted`

```xml
<?xml version="1.0" encoding="utf-8"?>
<ReservationRequest>
    <OrderNumber>order-451</OrderNumber>
    <Positions>
        <Position>
            <ItemCode>SKU-001</ItemCode>
            <Quantity>2</Quantity>
        </Position>
    </Positions>
</ReservationRequest>
```

| Source `order.created` field | Warehouse XML field |
| --- | --- |
| `payload.order_id` | `ReservationRequest/OrderNumber` |
| `payload.items[*].product_code` | `Positions/Position/ItemCode` |
| `payload.items[*].quantity` | `Positions/Position/Quantity` |

Name, price, customer and shipping address do not belong to the current Warehouse request contract.

## Semantics

`202 Accepted` means that Warehouse 1С accepted the `ReservationRequest` for asynchronous processing. It does not mean that stock was successfully reserved.

The later reservation outcome is sent by Warehouse as `warehouse.reservation.succeeded` or `warehouse.reservation.failed`. Its route to Mini Shop is deliberately outside Milestone 2.

## Identifier policy

In the current Mini Shop implementation, `payload.order_id` is a UUID v4. It is
therefore the exact technical identifier sent as `OrderNumber` and stored by
Warehouse as the external order identifier. This provides unambiguous tracing
and idempotency across Mini Shop, Platform and 1С. A future short, human-readable
order number must be an additional field rather than a replacement for this
technical identifier.
