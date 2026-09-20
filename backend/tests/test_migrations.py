from pathlib import Path

from alembic import command
from alembic.config import Config


def test_migrations_upgrade_a_clean_database(monkeypatch) -> None:
    database_url = "sqlite+pysqlite:///:memory:"
    monkeypatch.setenv("DATABASE_URL", database_url)
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    config.set_main_option("script_location", str(Path(__file__).parents[1] / "migrations"))

    command.upgrade(config, "head")
