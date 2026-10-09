# Mini Shop Reservation Result v1

This is the target JSON contract sent by Integration Platform to Mini Shop when
Warehouse 1С reports the result of an asynchronous reservation.

```text
Warehouse 1С XML callback
  → Integration Platform Warehouse adapter
  → POST Mini Shop /api/integration/reservation-results
```

Mini Shop owns the resulting `Order` state transition. Platform transmits only
the integration fact and never decides `CONFIRMED` versus `REJECTED` itself.

## Request

- Method: `POST`
- URL: `{MINI_SHOP_BASE_URL}/api/integration/reservation-results`
- Content-Type: `application/json`
- Header: `Idempotency-Key: <WarehouseEvent/EventId>`

### Reservation succeeded

```json
{
  "order_id": "6a685f8f-6ece-45e5-b099-2d91ea9c2099",
  "reservation_result": "succeeded",
  "warehouse_event_id": "e8a5000b-ce59-4427-8cfe-0b55e5315b7c"
}
```

### Reservation failed

```json
{
  "order_id": "fbfee07b-9a9d-4b0e-a814-6c50d4c63371",
  "reservation_result": "failed",
  "warehouse_event_id": "367c5758-acdd-4e47-ab3f-fe50758f87ac",
  "reason_code": "OUT_OF_STOCK",
  "message": "Insufficient stock for product Example Product. Available: 0, requested: 1"
}
```

## Field mapping

| Mini Shop field | Warehouse canonical callback field |
| --- | --- |
| `order_id` | `payload.external_order_id` |
| `reservation_result = succeeded` | `event_type = warehouse.reservation.succeeded` |
| `reservation_result = failed` | `event_type = warehouse.reservation.failed` |
| `warehouse_event_id` | `ExternalEventEnvelope.event_id` |
| `reason_code` | failed callback `payload.reason_code` |
| `message` | failed callback `payload.message` |

`reason_code` and `message` are required only for `reservation_result = failed`.
They must be absent for `succeeded`.

## Mini Shop semantics

| Current order state | Result | HTTP outcome | New state |
| --- | --- | --- | --- |
| `WAITING_FOR_RESERVATION` | `succeeded` | `200` | `CONFIRMED` |
| `WAITING_FOR_RESERVATION` | `failed` | `200` | `REJECTED` |
| `CONFIRMED` | repeated `succeeded` | `200` | unchanged |
| `REJECTED` | repeated `failed` | `200` | unchanged |
| terminal state | conflicting result | `409` | unchanged |
| unknown `order_id` | either result | `404` | — |

Platform treats an HTTP `2xx` response as successful synchronous delivery. A
Warehouse business failure that produces `REJECTED` is still a successful
integration delivery, not a Platform delivery error.
