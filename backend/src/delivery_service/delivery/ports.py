from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from delivery_service.delivery.models import Delivery


@dataclass(frozen=True)
class DeliveryExecution:
    http_status: int | None


class DeliveryExecutorPort(Protocol):
    def execute(self, delivery: Delivery) -> DeliveryExecution: ...


class MessageCompletionPort(Protocol):
    def refresh(self, message_id: str) -> None: ...
