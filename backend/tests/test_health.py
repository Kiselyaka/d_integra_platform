from fastapi.testclient import TestClient

from integration_api.main import create_app as create_integration_app
from delivery_service.main import create_app as create_delivery_app


def test_integration_api_liveness() -> None:
    response = TestClient(create_integration_app()).get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_delivery_service_liveness() -> None:
    response = TestClient(create_delivery_app()).get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_uses_database_connection() -> None:
    response = TestClient(create_integration_app()).get("/health/ready")
    assert response.status_code == 200
    assert response.json()["database"] == "available"

