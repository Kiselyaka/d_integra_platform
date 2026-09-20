# Integration Platform

Backend and Admin UI for the diploma Integration Platform. Milestone 0 only provides service startup, PostgreSQL readiness checks and an Admin UI placeholder.

Run the complete workspace from `../integration-environment` with `make up` once that repository's Compose environment is in place. The Integration API applies the shared platform migration; Delivery Service uses the same prepared `integration_db` and does not race it at startup.

Local backend checks:

```sh
cd backend
python -m pip install -e '.[dev]'
pytest
DATABASE_URL=<local PostgreSQL URL> alembic upgrade head
```

Both processes expose `GET /health/live` and `GET /health/ready`; readiness executes `SELECT 1` against `DATABASE_URL`. The repository deliberately contains no event ingestion, routing, delivery, retry, Kafka consumer or business-domain implementation yet.
