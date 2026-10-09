# Milestone 3 — полный бизнес-цикл через Warehouse callback

**Статус:** завершён и проверен на локальных сервисах и рабочей информационной
базе Warehouse 1С.

## Цель

M3 закрывает обратную половину reservation flow: результат, который Warehouse
1С публикует после асинхронной обработки заявки, должен изменить состояние
исходного заказа в Mini Shop.

```text
Mini Shop order.created
  → Integration Platform
       ├─ CRM
       └─ Warehouse 1С ReservationRequest
             → reservation decision
             → WarehouseEvent XML callback
                 → Integration Platform
                     → Mini Shop reservation-result JSON
                         → Order state transition
```

Integration Platform передаёт факт интеграции. Бизнес-правило
`succeeded → CONFIRMED`, `failed → REJECTED` принадлежит Mini Shop.

## Warehouse 1С: подтверждённый source contract

Конфигурация 1С не изменялась. Реальный контракт M1+ уже полностью подходил
для M3:

| Wire XML 1С | Canonical event Platform |
| --- | --- |
| `reservation.succeeded` | `warehouse.reservation.succeeded` |
| `reservation.failed` | `warehouse.reservation.failed` |

Envelope `WarehouseEvent` содержит `EventId`, `EventType`, `OccurredAt`,
`CorrelationId`, `SchemaVersion` и `Payload`. Platform сохраняет
`CorrelationId`; идентификатор заказа берётся из
`Payload/ReservationSucceeded|Failed/OrderNumber` и становится
`payload.external_order_id`.

Для неуспеха доступны фактические поля:

```json
{
  "external_order_id": "…",
  "status": "FAILED",
  "reason_code": "OUT_OF_STOCK",
  "message": "Insufficient stock for product Second Product. Available: 1, requested: 10"
}
```

**No Warehouse 1C changes required.**

## Platform routes

M3 добавляет ровно два статических маршрута:

| Source event | Route key | Target |
| --- | --- | --- |
| `warehouse-1c / warehouse.reservation.succeeded / v1` | `warehouse-reservation-succeeded-to-mini-shop` | Mini Shop |
| `warehouse-1c / warehouse.reservation.failed / v1` | `warehouse-reservation-failed-to-mini-shop` | Mini Shop |

Каждый callback остаётся отдельным `Message`; исходный `order.created Message`
не переиспользуется. Inbound idempotency callback продолжает действовать по
`(source_system, external_event_id)`.

## Target contract Mini Shop

Опубликован отдельный контракт:
[Mini Shop Reservation Result v1](../contracts/mini-shop-reservation-result-v1.md).

Platform вызывает:

```text
POST http://mini-shop:8002/api/integration/reservation-results
Content-Type: application/json
Idempotency-Key: <Warehouse EventId>
```

Успешный результат:

```json
{
  "order_id": "6a685f8f-6ece-45e5-b099-2d91ea9c2099",
  "reservation_result": "succeeded",
  "warehouse_event_id": "e8a5000b-ce59-4427-8cfe-0b55e5315b7c"
}
```

Неуспешный результат передаёт дополнительно `reason_code` и `message`.
Platform не интерпретирует `FAILED` как техническую ошибку: успешная доставка
факта отказа в Mini Shop даёт `Message = COMPLETED`.

## Mini Shop state machine

Новый заказ сразу получает `WAITING_FOR_RESERVATION`.

| Состояние | Входящий факт | Результат |
| --- | --- | --- |
| `WAITING_FOR_RESERVATION` | `succeeded` | `CONFIRMED` |
| `WAITING_FOR_RESERVATION` | `failed` | `REJECTED` |
| `CONFIRMED` | повтор `succeeded` | no-op, `200` |
| `REJECTED` | повтор `failed` | no-op, `200` |
| terminal state | противоположный результат | `409 Conflict` |

Неизвестный `order_id` получает `404`. Отдельный Inbox не добавлялся:
Platform дедуплицирует один и тот же `WarehouseEvent/EventId`, а Mini Shop
дополнительно безопасен для повторного результата того же типа.

## Изменения по репозиториям

### `integration-platform`

- `StaticRouteProvider` расширен двумя Warehouse callback route.
- Добавлены преобразования Warehouse succeeded/failed → Mini Shop JSON.
- Добавлен `MiniShopHttpTargetDeliveryAdapter` и dispatch по `target_system = mini-shop`.
- Добавлены `MINI_SHOP_BASE_URL` и timeout; Docker использует внутренний адрес
  `http://mini-shop:8002`.
- Добавлены route, transformation, adapter и pipeline tests.

### `demo-business-systems/mini-shop`

- Добавлены `OrderStatus` и защищённый domain transition.
- Добавлен endpoint `/api/integration/reservation-results`.
- Добавлены target Pydantic contracts и controlled `404`/`409` responses.
- Миграция `20261009_0003` переводит старый default `CREATED` в
  `WAITING_FOR_RESERVATION` и меняет default для новых заказов.
- Миграция `20261009_0004` добавляет reproducible demo product
  `SKU-002 / Second Product / 10.50 EUR` для failure E2E.

### `integration-environment`

- В Integration API добавлен `MINI_SHOP_BASE_URL=http://mini-shop:8002`.

### `warehouse-1c`

- Нет изменений кода, контракта или конфигурации.

## Проверки

### Автоматические

```text
Mini Shop M3 application + migration tests: 10 passed
Integration Platform M2 + M3 selected tests: 21 passed
```

Полный host-run тестов, использующих FastAPI `TestClient`, в текущем Windows
окружении может зависать ещё при создании AnyIO socketpair. Новые M3
application-level тесты не зависят от этого ограничения; HTTP interaction
подтверждена ниже настоящими Docker E2E.

### Реальный happy path

```text
Order id:           6a685f8f-6ece-45e5-b099-2d91ea9c2099
order.created:      3c2efde2-2742-482c-95f8-efca0b098b8e
Warehouse EventId:  e8a5000b-ce59-4427-8cfe-0b55e5315b7c
Callback Message:   fca9d8ad-b6ba-49ff-b2c1-35068552cc59
Callback route:     warehouse-reservation-succeeded-to-mini-shop
Final order status: CONFIRMED
```

Исходный `order.created` создал CRM deal и Warehouse `ReservationRequest`.
Warehouse callback был принят Platform, Mini Shop ответил `200`, а callback
`Message` получил `COMPLETED`.

### Реальный failed path

```text
Order id:           dc6d42fa-b61e-4fa5-a995-d6c2b5390212
Product:            SKU-002 / Second Product / quantity 10
Warehouse EventId:  8f2a7ec8-7fee-4569-a3a5-037188128f35
Callback Message:   548f5dc4-32ae-4026-8c81-f21099454252
Callback route:     warehouse-reservation-failed-to-mini-shop
Warehouse reason:   OUT_OF_STOCK
Final order status: REJECTED
```

1С вернула: `Available: 1, requested: 10`. Это бизнесовый отказ, но
технически успешная интеграционная доставка: callback `Message = COMPLETED`,
route = `DELIVERY_REQUESTED`.

### Duplicate callback

Фактический succeeded callback `e8a5000b-ce59-4427-8cfe-0b55e5315b7c` был
повторно отправлен в Platform в неизменном семантическом виде. Ответ:

```json
{
  "message_id": "fca9d8ad-b6ba-49ff-b2c1-35068552cc59",
  "status": "already_accepted"
}
```

После повтора осталось ровно по одной записи `Message` и
`RouteProcessingResult`; заказ остался `CONFIRMED`.

### Промежуточный HTTP 500

Первый запрос с `SKU-002` временно получил HTTP `500` от веб-сервера 1С из-за
ограничения подключений. Это была техническая ошибка транспорта, а не
`reservation.failed` и не несовместимость XML-контракта. Повторный запрос был
принят 1С с `202`; последующий successful и failed E2E подтвердили контракт.

## Ограничения

- Platform delivery по-прежнему синхронная, без retry, delivery aggregate,
  DLQ или worker.
- Mini Shop пока не имеет Inbox persistence; его idempotent state transitions
  дополняют inbound deduplication Platform.
- Kafka и Platform outbox/inbox не участвуют в runtime flow.
- Static routes ещё не вынесены в configuration/UI.
- M4 автоматически не начинается.
