from delivery_service.main import create_app as create_delivery_app
from integration_api.main import create_app as create_integration_app
from shared.database import create_database_engine, database_is_ready


def _paths(app: object) -> set[str]:
    paths: set[str] = set()
    for route in getattr(app, "routes"):
        path = getattr(route, "path", None)
        if path is not None:
            paths.add(path)
        original_router = getattr(route, "original_router", None)
        if original_router is not None:
            paths.update(child.path for child in original_router.routes)
    return paths


def test_integration_api_exposes_health_routes() -> None:
    assert {"/health/live", "/health/ready"}.issubset(_paths(create_integration_app()))


def test_delivery_service_exposes_health_routes() -> None:
    assert {"/health/live", "/health/ready"}.issubset(_paths(create_delivery_app()))


def test_readiness_uses_database_connection() -> None:
    assert database_is_ready(create_database_engine("sqlite+pysqlite:///:memory:")) is True
