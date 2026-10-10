# Integration Platform

Backend and Admin UI for the diploma Integration Platform. Milestones 1–3
implement the Mini Shop, CRM and Warehouse 1С business flows. Milestones 4–5
add the Platform-owned Delivery runtime and its at-least-once boundary:
immutable target snapshots, attempts, retry, dead-letter handling, optimistic
claims, stale-processing recovery, transactional Outbox, Kafka and Inbox
deduplication. Milestones 6–8 make routes, transformations and contracts
data-driven, versioned, validated and previewable. Milestone 9 adds the
read-only Operational Query API and an explicit, lineage-preserving recovery
command for dead-letter Deliveries.

Run the complete workspace from `../integration-environment` with `make up` once that repository's Compose environment is in place. The Integration API applies the shared platform migration and idempotently seeds missing demo configuration; Delivery Service uses the same prepared `integration_db` and does not race it at startup.

Local backend checks:

```sh
cd backend
python -m pip install -e '.[dev]'
pytest
DATABASE_URL=<local PostgreSQL URL> alembic upgrade head
```

Both processes expose `GET /health/live` and `GET /health/ready`; readiness executes `SELECT 1` against `DATABASE_URL`. `integration-api` reads matching routes from Configuration Context, writes `delivery.requested` to Processing Outbox and consumes normal terminal delivery events. `delivery-service` consumes requested deliveries, performs the queued/retryable poll and publishes terminal facts. A recovery Delivery created by the M9 requeue command inherits the immutable original snapshot, is picked up by the same poller, and emits separate `delivery.recovery.*` events; it never rewrites the historical Message result.
