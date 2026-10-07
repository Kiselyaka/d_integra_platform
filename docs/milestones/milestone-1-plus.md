# Milestone 1+ — приём событий Warehouse 1С

**Статус:** завершён.

Milestone 1+ — промежуточный интеграционный этап после Milestone 1 и до Milestone 2. Его цель — завершить вертикальный путь **Warehouse 1С → Integration Platform** без добавления маршрута в Mini Shop.

## Граница этапа

В Milestone 1 уже существовал универсальный JSON endpoint `POST /api/v1/events` и pipeline приёма сообщений: проверка источника, идемпотентное сохранение `Message` и запуск обработки.

В M1+ добавлен inbound adapter для Warehouse 1С:

```text
OutboundEvents в Warehouse 1С
  → HTTP POST XML
  → POST /api/v1/events/warehouse
  → warehouse_xml adapter
  → ExternalEventEnvelope
  → существующий ingestion pipeline
  → integration_db.messages
```

Apache, опубликованный HTTP-сервис 1С и endpoint `:8081` не участвуют в этой исходящей отправке. Они обслуживают отдельный входящий путь в 1С.

## Архитектурное решение

Warehouse 1С остаётся владельцем своего внешнего доменного контракта. Platform не требует, чтобы 1С формировала внутренний `ExternalEventEnvelope` или переименовывала свои поля.

```text
Warehouse domain contract
  → Warehouse XML adapter в Platform
  → canonical ExternalEventEnvelope
  → Message / routing pipeline
```

Следствия решения:

- 1С публикует `ReservationSucceeded` и `ReservationFailed` с собственными полями `OrderNumber`, `Status`, `Code`, `Message`.
- Transport metadata (`EventId`, `EventType`, `OccurredAt`, `CorrelationId`, `SchemaVersion`) передаётся в обёртке `WarehouseEvent`.
- Adapter в Platform нормализует внешний контракт для внутренней обработки.
- Mini Shop не зависит от XML-контракта 1С.

## Внешний контракт Warehouse 1С

Endpoint: `POST /api/v1/events/warehouse`
Content-Type: `application/xml`

### Успешное резервирование

```xml
<?xml version="1.0" encoding="UTF-8"?>
<WarehouseEvent>
    <EventId>563e6de0-329f-47f5-8567-31bb3a431b47</EventId>
    <EventType>reservation.succeeded</EventType>
    <OccurredAt>2026-10-07T18:00:00+03:00</OccurredAt>
    <CorrelationId>order-705</CorrelationId>
    <SchemaVersion>1</SchemaVersion>
    <Payload>
        <ReservationSucceeded>
            <OrderNumber>705</OrderNumber>
            <Status>SUCCEEDED</Status>
        </ReservationSucceeded>
    </Payload>
</WarehouseEvent>
```

### Неуспешное резервирование

```xml
<WarehouseEvent>
    <EventId>367c5758-acdd-4e47-ab3f-fe50758f87ac</EventId>
    <EventType>reservation.failed</EventType>
    <OccurredAt>2026-10-07T18:00:00+03:00</OccurredAt>
    <CorrelationId>order-707</CorrelationId>
    <SchemaVersion>1</SchemaVersion>
    <Payload>
        <ReservationFailed>
            <OrderNumber>707</OrderNumber>
            <Status>FAILED</Status>
            <Code>OUT_OF_STOCK</Code>
            <Message>Insufficient stock for product Example Product.</Message>
        </ReservationFailed>
    </Payload>
</WarehouseEvent>
```

## Нормализация в Platform

| Внешний Warehouse XML | Canonical Event в Platform |
| --- | --- |
| `EventType = reservation.succeeded` | `source_system = warehouse-1c`, `event_type = warehouse.reservation.succeeded` |
| `ReservationSucceeded/OrderNumber` | `payload.external_order_id` |
| `ReservationSucceeded/Status` | `payload.status` |
| `EventType = reservation.failed` | `source_system = warehouse-1c`, `event_type = warehouse.reservation.failed` |
| `ReservationFailed/OrderNumber` | `payload.external_order_id` |
| `ReservationFailed/Status` | `payload.status` |
| `ReservationFailed/Code` | `payload.reason_code` |
| `ReservationFailed/Message` | `payload.message` |

Для обоих вариантов `EventId` становится `external_event_id`, `OccurredAt` — `occurred_at`, `CorrelationId` — `correlation_id`, `SchemaVersion` — `schema_version`.

## Реализовано в Integration Platform

- Добавлен endpoint `POST /api/v1/events/warehouse`.
- Добавлен XML adapter `warehouse_xml.py`; endpoint не обращается к SQLAlchemy или `MessageRepository` напрямую.
- Новый endpoint использует тот же `EventIngestionService`, что и generic endpoint `POST /api/v1/events`.
- В `PydanticSourceValidator` добавлены два Warehouse source contract:
  - `warehouse-1c / warehouse.reservation.succeeded / 1`;
  - `warehouse-1c / warehouse.reservation.failed / 1`.
- Добавлены payload models `WarehouseReservationSucceededV1` и `WarehouseReservationFailedV1`.
- Повторный XML с той же парой `(source_system = warehouse-1c, EventId)` возвращает `202 already_accepted` и не создаёт второй `Message`.
- В пример добавлен [`warehouse-event.xml`](../../examples/warehouse-event.xml).

## Обработка ошибок

Adapter возвращает `400 Bad Request`, а не `500`, для:

- malformed XML;
- отсутствующих `EventId`, `EventType`, `OccurredAt`, `SchemaVersion` или `Payload`;
- неподдерживаемого внешнего `EventType`;
- отсутствующих обязательных полей бизнес-payload.

Source validation после нормализации остаётся обязанностью существующего validation layer.

## Результат обработки и намеренные ограничения

После успешного ingestion событие Warehouse сейчас сохраняется со статусом `NO_ROUTE`. Это ожидаемо: M1+ не добавляет ни трансформацию Warehouse → Mini Shop, ни delivery, retry, Kafka, outbox/inbox на стороне Platform.

Вне рамок M1+:

- маршрут Warehouse → Mini Shop;
- отправка результата в Mini Shop;
- delivery aggregate и retry в Platform;
- Kafka и Platform outbox/inbox;
- изменение доменного payload Warehouse 1С.

## Проверка

Автотесты backend:

```text
python -m pytest -q
28 passed
```

Проверены XML adapter и HTTP endpoint для succeeded/failed, malformed XML, отсутствующих полей, неподдерживаемого типа, невалидного payload и duplicate `EventId`.

Реальная smoke-проверка выполнена через `http://127.0.0.1:8000/api/v1/events/warehouse`:

- заказ `705` сохранён как `warehouse.reservation.succeeded`;
- заказ `707` сохранён как `warehouse.reservation.failed`;
- заказ `708` сохранён как `warehouse.reservation.succeeded`;
- все записи имеют `source_system = warehouse-1c` и `status = NO_ROUTE`;
- повторные доставки одного `EventId` не создали дубли.

## Условие готовности для 1С

Platform готова принимать события dispatcher из 1С. Чтобы включить сквозной путь, в Warehouse 1С требуется активное регламентное задание:

```text
ProcessOutboundEvents
→ ReservationJobs.ProcessOutboundEvents()
→ OutboundEvents(Pending)
→ POST /api/v1/events/warehouse
→ 202
→ OutboundEvents(Sent)
```

Если серверный код 1С и Docker работают на одном Windows-хосте, dispatcher использует адрес `http://127.0.0.1:8000/api/v1/events/warehouse`. Если это разные машины, используется IP/hostname машины с Docker вместо `127.0.0.1`.
