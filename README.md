# Integration Platform

Backend and Admin UI for the diploma Integration Platform. Milestones 1–3
implement the Mini Shop, CRM and Warehouse 1С business flows. Milestone 4 adds
the Platform-owned Delivery runtime: immutable target snapshots, attempts,
retry, dead-letter handling, optimistic claims and stale-processing recovery.

Run the complete workspace from `../integration-environment` with `make up` once that repository's Compose environment is in place. The Integration API applies the shared platform migration; Delivery Service uses the same prepared `integration_db` and does not race it at startup.

Local backend checks:

```sh
cd backend
python -m pip install -e '.[dev]'
pytest
DATABASE_URL=<local PostgreSQL URL> alembic upgrade head
```

Both processes expose `GET /health/live` and `GET /health/ready`; readiness executes `SELECT 1` against `DATABASE_URL`. `integration-api` applies migrations and creates Delivery requests after successful route transformation. `delivery-service` performs the local M4 poll for queued/retryable Delivery records. Kafka, Platform Outbox/Inbox and manual requeue remain later milestones.
