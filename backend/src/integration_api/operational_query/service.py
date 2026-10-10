"""On-demand, read-only operational projections for M9."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from delivery_service.delivery.models import Delivery, DeliveryAttempt
from integration_api.message_processing.models import Message, RouteProcessingResult
from integration_api.operational_query.outcome import derive_outcome
from integration_api.operational_query.pagination import Cursor, encode_cursor


class OperationalNotFoundError(ValueError):
    pass


class OperationalQueryService:
    """Build DTO dictionaries from facts without mutating any aggregate."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def list_messages(
        self,
        *,
        limit: int,
        cursor: Cursor | None,
        source_system: str | None = None,
        event_type: str | None = None,
        status: str | None = None,
        outcome: str | None = None,
        correlation_id: str | None = None,
        external_event_id: str | None = None,
        message_id: str | None = None,
        received_from: datetime | None = None,
        received_to: datetime | None = None,
    ) -> dict[str, Any]:
        with self._session_factory() as session:
            statement = select(Message)
            for condition in (
                Message.source_system == source_system if source_system else None,
                Message.event_type == event_type if event_type else None,
                Message.status == status if status else None,
                Message.correlation_id == correlation_id if correlation_id else None,
                Message.external_event_id == external_event_id if external_event_id else None,
                Message.id == message_id if message_id else None,
                Message.received_at >= received_from if received_from else None,
                Message.received_at <= received_to if received_to else None,
            ):
                if condition is not None:
                    statement = statement.where(condition)
            if cursor is not None:
                statement = statement.where(or_(
                    Message.received_at < cursor.timestamp,
                    and_(Message.received_at == cursor.timestamp, Message.id < cursor.identifier),
                ))
            ordered = statement.order_by(Message.received_at.desc(), Message.id.desc())
            if outcome is None:
                rows = list(session.scalars(ordered.limit(limit + 1)))
                page_rows = rows[:limit]
                next_cursor = (
                    encode_cursor(page_rows[-1].received_at, page_rows[-1].id)
                    if len(rows) > limit and page_rows
                    else None
                )
                return {"items": self._message_summaries(session, page_rows), "next_cursor": next_cursor}

            # ``outcome`` is deliberately derived from historic RPR/Delivery
            # facts, not denormalised onto Message.  Scan in bounded batches
            # until we have one extra match: it keeps the opaque cursor correct
            # without an N+1 query or dropping matching messages behind a page
            # full of non-matching ones.
            matches: list[tuple[Message, dict[str, Any]]] = []
            scan_cursor = cursor
            batch_size = max(50, limit * 2)
            exhausted = False
            while len(matches) <= limit and not exhausted:
                batch_statement = ordered
                if scan_cursor is not None:
                    batch_statement = batch_statement.where(or_(
                        Message.received_at < scan_cursor.timestamp,
                        and_(Message.received_at == scan_cursor.timestamp, Message.id < scan_cursor.identifier),
                    ))
                batch = list(session.scalars(batch_statement.limit(batch_size)))
                exhausted = len(batch) < batch_size
                for row, summary in zip(batch, self._message_summaries(session, batch), strict=True):
                    if summary["derived_outcome"] == outcome:
                        matches.append((row, summary))
                        if len(matches) > limit:
                            break
                if batch:
                    scan_cursor = Cursor(batch[-1].received_at, batch[-1].id)

            page = matches[:limit]
            return {
                "items": [summary for _, summary in page],
                "next_cursor": (
                    encode_cursor(page[-1][0].received_at, page[-1][0].id)
                    if len(matches) > limit and page
                    else None
                ),
            }

    def message_details(self, message_id: str) -> dict[str, Any]:
        with self._session_factory() as session:
            message = session.get(Message, message_id)
            if message is None:
                raise OperationalNotFoundError("Message was not found")
            routes = list(session.scalars(select(RouteProcessingResult).where(RouteProcessingResult.message_id == message.id).order_by(RouteProcessingResult.started_at, RouteProcessingResult.id)))
            deliveries = list(session.scalars(select(Delivery).where(Delivery.message_id == message.id).order_by(Delivery.created_at, Delivery.id)))
            summary = self._message_summaries(session, [message], routes_by_message={message.id: routes}, deliveries_by_message={message.id: deliveries})[0]
            return {
                **summary,
                "occurred_at": message.occurred_at,
                "schema_version": message.schema_version,
                "validation_error": message.validation_error,
                "canonical_payload": message.payload,
                "raw_payload": None,
                "raw_payload_available": False,
                "route_results": [_route_dto(item) for item in routes],
                "deliveries": [_delivery_summary(item) for item in deliveries],
            }

    def message_timeline(self, message_id: str) -> dict[str, Any]:
        with self._session_factory() as session:
            message = session.get(Message, message_id)
            if message is None:
                raise OperationalNotFoundError("Message was not found")
            deliveries = list(session.scalars(select(Delivery).where(Delivery.message_id == message.id)))
            routes = list(session.scalars(select(RouteProcessingResult).where(RouteProcessingResult.message_id == message.id)))
            attempts = self._attempts_for(session, deliveries)
            return {"message_id": message.id, "items": _timeline([message], routes, deliveries, attempts)}

    def correlation_trace(self, correlation_id: str) -> dict[str, Any]:
        with self._session_factory() as session:
            messages = list(session.scalars(select(Message).where(Message.correlation_id == correlation_id).order_by(Message.received_at, Message.id)))
            deliveries = list(session.scalars(select(Delivery).where(Delivery.correlation_id == correlation_id).order_by(Delivery.created_at, Delivery.id)))
            if not messages and not deliveries:
                raise OperationalNotFoundError("Correlation was not found")
            message_ids = [item.id for item in messages]
            routes = list(session.scalars(select(RouteProcessingResult).where(RouteProcessingResult.message_id.in_(message_ids)))) if message_ids else []
            attempts = self._attempts_for(session, deliveries)
            return {
                "correlation_id": correlation_id,
                "messages": self._message_summaries(session, messages, routes_by_message=_group_by(routes, "message_id"), deliveries_by_message=_group_by(deliveries, "message_id")),
                "deliveries": [_delivery_summary(item) for item in deliveries],
                "timeline": _timeline(messages, routes, deliveries, attempts),
            }

    def list_deliveries(
        self,
        *,
        limit: int,
        cursor: Cursor | None,
        target_system: str | None = None,
        route: str | None = None,
        status: str | None = None,
        error_category: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        dead_letters_only: bool = False,
    ) -> dict[str, Any]:
        with self._session_factory() as session:
            statement = select(Delivery)
            conditions = (
                Delivery.target_system == target_system if target_system else None,
                Delivery.route_key == route if route else None,
                Delivery.status == status if status else None,
                Delivery.last_error_category == error_category if error_category else None,
                Delivery.correlation_id == correlation_id if correlation_id else None,
                Delivery.message_id == message_id if message_id else None,
                Delivery.created_at >= created_from if created_from else None,
                Delivery.created_at <= created_to if created_to else None,
                Delivery.status == "DEAD_LETTER" if dead_letters_only else None,
            )
            for condition in conditions:
                if condition is not None:
                    statement = statement.where(condition)
            if cursor is not None:
                statement = statement.where(or_(
                    Delivery.created_at < cursor.timestamp,
                    and_(Delivery.created_at == cursor.timestamp, Delivery.id < cursor.identifier),
                ))
            rows = list(session.scalars(statement.order_by(Delivery.created_at.desc(), Delivery.id.desc()).limit(limit + 1)))
            page_rows = rows[:limit]
            next_cursor = encode_cursor(page_rows[-1].created_at, page_rows[-1].id) if len(rows) > limit and page_rows else None
            return {"items": [_delivery_summary(item) for item in page_rows], "next_cursor": next_cursor}

    def delivery_details(self, delivery_id: str) -> dict[str, Any]:
        with self._session_factory() as session:
            delivery = session.get(Delivery, delivery_id)
            if delivery is None:
                raise OperationalNotFoundError("Delivery was not found")
            route_result = session.get(RouteProcessingResult, delivery.route_processing_result_id)
            attempts = list(session.scalars(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == delivery.id).order_by(DeliveryAttempt.attempt_number)))
            lineage = _lineage(session, delivery)
            return {
                **_delivery_summary(delivery),
                "target_url": delivery.target_url,
                "http_method": delivery.http_method,
                "payload_format": delivery.payload_format,
                "transformed_payload": delivery.transformed_payload,
                "connect_timeout_ms": delivery.connect_timeout_ms,
                "read_timeout_ms": delivery.read_timeout_ms,
                "retry_snapshot": {
                    "max_attempts": delivery.max_attempts,
                    "strategy": delivery.retry_strategy,
                    "initial_delay_seconds": delivery.retry_initial_delay_seconds,
                    "max_delay_seconds": delivery.retry_max_delay_seconds,
                    "multiplier": delivery.retry_multiplier,
                    "retryable_errors": delivery.retryable_errors,
                },
                "historical_route_processing": _route_dto(route_result) if route_result is not None else None,
                # AuthProfile values are intentionally not snapshotted onto a
                # Delivery and secrets are never exposed by this API.
                "auth_profile": None,
                "attempts": [_attempt_dto(item) for item in attempts],
                "lineage": [_delivery_summary(item) for item in lineage],
            }

    def _message_summaries(
        self,
        session: Session,
        messages: list[Message],
        *,
        routes_by_message: dict[str, list[RouteProcessingResult]] | None = None,
        deliveries_by_message: dict[str, list[Delivery]] | None = None,
    ) -> list[dict[str, Any]]:
        if not messages:
            return []
        message_ids = [item.id for item in messages]
        routes_by_message = routes_by_message or _group_by(
            list(session.scalars(select(RouteProcessingResult).where(RouteProcessingResult.message_id.in_(message_ids)))), "message_id"
        )
        deliveries_by_message = deliveries_by_message or _group_by(
            list(session.scalars(select(Delivery).where(Delivery.message_id.in_(message_ids)))), "message_id"
        )
        summaries: list[dict[str, Any]] = []
        for message in messages:
            routes = routes_by_message.get(message.id, [])
            deliveries = deliveries_by_message.get(message.id, [])
            initial_deliveries = [item for item in deliveries if item.requeued_from_delivery_id is None]
            summaries.append({
                "message_id": message.id,
                "received_at": message.received_at,
                "source_system": message.source_system,
                "event_type": message.event_type,
                "external_event_id": message.external_event_id,
                "correlation_id": message.correlation_id,
                "domain_status": message.status,
                # Recovery executions are operational facts.  They must never
                # rewrite the outcome of the historical processing execution.
                "derived_outcome": derive_outcome(message.status, (item.status for item in routes), (item.status for item in initial_deliveries)),
                "route_count": len(routes),
                "delivery_count": len(deliveries),
                "initial_delivery_count": len(initial_deliveries),
                "recovery_delivery_count": len(deliveries) - len(initial_deliveries),
                "error_summary": message.validation_error or next((item.error for item in routes if item.error), None),
            })
        return summaries

    @staticmethod
    def _attempts_for(session: Session, deliveries: list[Delivery]) -> list[DeliveryAttempt]:
        if not deliveries:
            return []
        return list(session.scalars(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id.in_([item.id for item in deliveries]))))


def _group_by(items: list[Any], attribute: str) -> dict[str, list[Any]]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for item in items:
        grouped[getattr(item, attribute)].append(item)
    return grouped


def _route_dto(route: RouteProcessingResult) -> dict[str, Any]:
    return {
        "route_processing_result_id": route.id,
        "route_key": route.route_key,
        "route_version": route.route_version,
        "route_id": route.route_id,
        "transformation_id": route.transformation_id,
        "transformation_version": route.transformation_version,
        "source_validation_schema": _schema_ref(route.source_validation_schema_id, route.source_validation_schema_key, route.source_validation_schema_version),
        "target_validation_schema": _schema_ref(route.target_validation_schema_id, route.target_validation_schema_key, route.target_validation_schema_version),
        "status": route.status,
        "started_at": route.started_at,
        "completed_at": route.completed_at,
        "error": route.error,
        "error_stage": route.error_stage,
        "error_category": route.error_category,
        "error_details": route.error_details,
    }


def _schema_ref(identifier: str | None, key: str | None, version: int | None) -> dict[str, Any] | None:
    if identifier is None and key is None and version is None:
        return None
    return {"id": identifier, "key": key, "version": version}


def _delivery_summary(delivery: Delivery) -> dict[str, Any]:
    return {
        "delivery_id": delivery.id,
        "created_at": delivery.created_at,
        "target_system": delivery.target_system,
        "route_key": delivery.route_key,
        "route_version": delivery.route_version,
        "status": delivery.status,
        "is_terminal": delivery.status in {"DELIVERED", "DEAD_LETTER"},
        "attempt_count": delivery.attempt_count,
        "next_retry_at": delivery.next_retry_at,
        "completed_at": delivery.completed_at,
        "last_error": {
            "category": delivery.last_error_category,
            "code": delivery.last_error_code,
            "message": delivery.last_error_message,
        } if delivery.last_error_category or delivery.last_error_code or delivery.last_error_message else None,
        "dead_letter_reason": delivery.dead_letter_reason,
        "message_id": delivery.message_id,
        "route_processing_result_id": delivery.route_processing_result_id,
        "correlation_id": delivery.correlation_id,
        "idempotency_key": delivery.idempotency_key,
        "requeued_from_delivery_id": delivery.requeued_from_delivery_id,
    }


def _attempt_dto(attempt: DeliveryAttempt) -> dict[str, Any]:
    duration_ms = None
    if attempt.completed_at is not None:
        duration_ms = int((attempt.completed_at - attempt.started_at).total_seconds() * 1000)
    return {
        "attempt_number": attempt.attempt_number,
        "started_at": attempt.started_at,
        "completed_at": attempt.completed_at,
        "duration_ms": duration_ms,
        "outcome": attempt.outcome,
        "http_status": attempt.http_status,
        "error_category": attempt.error_category,
        "error_code": attempt.error_code,
        "error_message": attempt.error_message,
    }


def _lineage(session: Session, delivery: Delivery) -> list[Delivery]:
    current = delivery
    ancestors: list[Delivery] = []
    while current.requeued_from_delivery_id is not None:
        parent = session.get(Delivery, current.requeued_from_delivery_id)
        if parent is None:
            break
        ancestors.append(parent)
        current = parent
    ancestors.reverse()
    chain = ancestors + [delivery]
    current = delivery
    while True:
        child = session.scalar(select(Delivery).where(Delivery.requeued_from_delivery_id == current.id))
        if child is None:
            break
        chain.append(child)
        current = child
    return chain


def _timeline(
    messages: list[Message], routes: list[RouteProcessingResult], deliveries: list[Delivery], attempts: list[DeliveryAttempt]
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for message in messages:
        entries.append(_timeline_entry(message.received_at, "MESSAGE_RECEIVED", "message", message.id, message.status, message.event_type))
    for route in routes:
        entries.append(_timeline_entry(route.started_at, "ROUTE_PROCESSING_STARTED", "route_processing_result", route.id, route.status, route.route_key))
        if route.completed_at is not None:
            entries.append(_timeline_entry(route.completed_at, route.status, "route_processing_result", route.id, route.status, route.error_category))
    for delivery in deliveries:
        entries.append(_timeline_entry(delivery.created_at, "DELIVERY_CREATED", "delivery", delivery.id, delivery.status, delivery.target_system))
        if delivery.completed_at is not None:
            entries.append(_timeline_entry(delivery.completed_at, f"DELIVERY_{delivery.status}", "delivery", delivery.id, delivery.status, delivery.dead_letter_reason))
    for attempt in attempts:
        entries.append(_timeline_entry(attempt.started_at, "DELIVERY_ATTEMPT_STARTED", "delivery_attempt", attempt.id, attempt.outcome, str(attempt.attempt_number)))
        if attempt.completed_at is not None:
            entries.append(_timeline_entry(attempt.completed_at, f"DELIVERY_ATTEMPT_{attempt.outcome}", "delivery_attempt", attempt.id, attempt.outcome, attempt.error_code))
    priorities = {
        "MESSAGE_RECEIVED": 0,
        "ROUTE_PROCESSING_STARTED": 1,
        "DELIVERY_REQUESTED": 2,
        "SOURCE_VALIDATION_FAILED": 2,
        "TRANSFORMATION_FAILED": 2,
        "TARGET_VALIDATION_FAILED": 2,
        "CONFIGURATION_ERROR": 2,
        "DELIVERY_CREATED": 3,
        "DELIVERY_ATTEMPT_STARTED": 4,
        "DELIVERY_ATTEMPT_SUCCEEDED": 5,
        "DELIVERY_ATTEMPT_FAILED": 5,
        "DELIVERY_ATTEMPT_INTERRUPTED": 5,
        "DELIVERY_DELIVERED": 6,
        "DELIVERY_DEAD_LETTER": 6,
    }
    return sorted(entries, key=lambda item: (item["timestamp"], priorities.get(item["kind"], 2), item["entity_id"], item["kind"]))


def _timeline_entry(timestamp: datetime, kind: str, entity_type: str, entity_id: str, status: str | None, details: str | None) -> dict[str, Any]:
    return {"timestamp": timestamp, "kind": kind, "entity_type": entity_type, "entity_id": entity_id, "status": status, "details": details}
