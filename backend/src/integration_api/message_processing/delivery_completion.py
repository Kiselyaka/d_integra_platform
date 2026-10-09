from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from delivery_service.delivery.models import ACTIVE_DELIVERY_STATUSES, Delivery, DeliveryStatus
from delivery_service.delivery.ports import MessageCompletionPort
from integration_api.message_processing.models import Message, RouteProcessingResult


class MessageDeliveryCompletionService(MessageCompletionPort):
    """Derives existing Message terminal statuses from Delivery terminal states.

    M4 intentionally keeps the established Message status vocabulary.  A
    validated Message remains non-terminal while one of its Deliveries is
    active; it becomes COMPLETED only once every requested Delivery succeeds.
    """

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def refresh(self, message_id: str) -> None:
        with self._session_factory() as session:
            message = session.get(Message, message_id)
            if message is None or message.status in {"NO_ROUTE", "VALIDATION_FAILED"}:
                return
            results = list(
                session.scalars(select(RouteProcessingResult).where(RouteProcessingResult.message_id == message_id))
            )
            deliveries = list(session.scalars(select(Delivery).where(Delivery.message_id == message_id)))
            if any(DeliveryStatus(delivery.status) in ACTIVE_DELIVERY_STATUSES for delivery in deliveries):
                message.status = "VALIDATED"
            elif results and all(result.status == "DELIVERY_REQUESTED" for result in results) and len(deliveries) == len(results) and all(
                delivery.status == DeliveryStatus.DELIVERED for delivery in deliveries
            ):
                message.status = "COMPLETED"
            else:
                message.status = "ROUTING_FAILED"
            session.commit()
