"""Opaque, stable cursor helpers for operational lists."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime


class InvalidCursorError(ValueError):
    pass


@dataclass(frozen=True)
class Cursor:
    timestamp: datetime
    identifier: str


def encode_cursor(timestamp: datetime, identifier: str) -> str:
    # SQLite host tests return naive DateTime values while PostgreSQL returns
    # timezone-aware values. API cursors are always normalized to UTC.
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    payload = json.dumps({"t": timestamp.isoformat(), "id": identifier}, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_cursor(value: str | None) -> Cursor | None:
    if value is None:
        return None
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        payload = json.loads(decoded)
        timestamp = datetime.fromisoformat(payload["t"])
        identifier = payload["id"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise InvalidCursorError("Cursor is invalid") from exc
    if timestamp.tzinfo is None or not isinstance(identifier, str) or not identifier:
        raise InvalidCursorError("Cursor is invalid")
    return Cursor(timestamp, identifier)
