# Local technical documentation

Architecture decision records belong in `adr/`; published contracts belong in `contracts/`.

Milestone documentation:

- [Milestone 0 — стартовая инфраструктура](milestones/milestone-0.md)
- [Milestone 1 — итоговый отчёт реализации](milestones/milestone-1.md)
- [Milestone 1+ — Warehouse 1С inbound events](milestones/milestone-1-plus.md)
- [Milestone 2 — fan-out `order.created` в CRM и Warehouse 1С](milestones/milestone-2.md)
- [Milestone 3 — полный бизнес-цикл через Warehouse callback](milestones/milestone-3.md)
- [Milestone 4 — Delivery, retry и stale recovery](milestones/milestone-4.md)
- [Milestone 5 — Kafka, transactional Outbox и Inbox](milestones/milestone-5.md)
- [Milestone 6 — Configuration Context](milestones/milestone-6.md)

Published contracts:

- [Warehouse ReservationRequest v1](contracts/warehouse-reservation-request-v1.md)
- [Mini Shop Reservation Result v1](contracts/mini-shop-reservation-result-v1.md)

Published Milestone 1 contracts:

- [External Event Envelope v1](contracts/external-event-envelope-v1.md)
- [Mini Shop `order.created` v1](contracts/mini-shop-order-created-v1.md)
