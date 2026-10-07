"""Public HTTP contracts owned by the Integration Platform."""

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ExternalEventEnvelope(BaseModel):
    """Versioned envelope for an event submitted by an external system."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    source_system: str = Field(min_length=1)
    occurred_at: datetime
    correlation_id: str | None = Field(default=None, min_length=1)
    schema_version: int = Field(ge=1)
    payload: dict[str, Any]


class OrderCreatedCustomer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: str = Field(min_length=1)
    full_name: str = Field(min_length=1)
    email: str = Field(min_length=1)
    phone: str | None = None


class OrderCreatedAddress(BaseModel):
    model_config = ConfigDict(extra="forbid")
    country: str = Field(min_length=1)
    city: str = Field(min_length=1)
    postal_code: str = Field(min_length=1)
    street: str = Field(min_length=1)
    building: str = Field(min_length=1)
    apartment: str | None = None


class OrderCreatedItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    product_code: str = Field(min_length=1)
    name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: Decimal


class OrderCreatedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str = Field(min_length=1)
    customer: OrderCreatedCustomer
    shipping_address: OrderCreatedAddress
    items: list[OrderCreatedItem] = Field(min_length=1)
    currency: str = Field(min_length=3, max_length=3)
    total_amount: Decimal


class WarehouseReservationSucceededV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_order_id: str = Field(min_length=1)
    status: str = Field(min_length=1)


class WarehouseReservationFailedV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_order_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    reason_code: str = Field(min_length=1)
    message: str = Field(min_length=1)
