# External Event Envelope v1

The Integration Platform accepts external events at `POST /api/v1/events`.

```json
{
  "event_id": "event-451",
  "event_type": "order.created",
  "source_system": "mini-shop",
  "occurred_at": "2026-10-04T12:00:00Z",
  "correlation_id": "order-451",
  "schema_version": 1,
  "payload": {}
}
```

| Field | Required | Rule |
| --- | --- | --- |
| `event_id` | yes | Non-empty external event identifier. |
| `event_type` | yes | Non-empty event type. |
| `source_system` | yes | Non-empty source system code. |
| `occurred_at` | yes | ISO-8601 timestamp. |
| `correlation_id` | no | Non-empty when supplied. Mini Shop supplies `order-<order_id>`. |
| `schema_version` | yes | Positive integer. |
| `payload` | yes | JSON object. |

For Milestone 1, the supported event is `mini-shop` / `order.created` / schema version `1`. The event-specific payload is documented separately before the Mini Shop implementation. This envelope is a published JSON contract; Mini Shop and CRM do not import Platform Python code to use it.
