# Milestone 4 — Delivery, retry и stale recovery

## 1. Цель Milestone 4

M4 заменяет временную синхронную доставку M1–M3 на самостоятельную domain
model доставки. Внешние соглашения Mini Shop, CRM и Warehouse 1С не меняются:
сохраняются существующие JSON/XML-контракты, URL, маршруты и ownership
бизнес-состояний.

```text
Message
  -> RouteProcessingResult (DELIVERY_REQUESTED)
  -> Delivery
  -> DeliveryAttempt
  -> HTTP target
```

`RouteProcessingResult` теперь означает только успешный route processing:
маршрут найден, payload преобразован и логическая Delivery создана. Он больше
не хранит transport lifecycle.

## 2. Состояние до M4 и новая граница контекстов

До M4 Integration API выполнял весь исходящий путь прямо во время HTTP-запроса,
принимающего событие:

```text
POST /api/v1/events
  -> Message(VALIDATED)
  -> RouteProcessingResult(PROCESSING)
  -> transformation
  -> synchronous HTTP POST
  -> RouteProcessingResult(DELIVERY_REQUESTED | DELIVERY_FAILED)
  -> Message(COMPLETED | ROUTING_FAILED)
```

В такой модели не было отдельной persistency-единицы доставки, истории попыток,
retry schedule, optimistic claim и recovery после падения worker.

После M4 границы следующие:

```text
Message Processing                         Delivery Context
------------------                         ----------------
Message -> RouteProcessingResult  ---->   Delivery -> DeliveryAttempt -> HTTP
          DELIVERY_REQUESTED               retry / dead letter / recovery
```

Message Processing обращается к локальному application port
`DeliveryRequestService`, а не к низкоуровневому Delivery repository. Это
временная local boundary M4. В M5 её можно заменить на
`Processing Outbox -> Kafka -> Delivery Inbox`, не переписывая aggregate
Delivery.

В M4 не изменялись конфигурация Warehouse 1С, Mini Shop, CRM, правила route
matching, XML-mapping и бизнес-ownership M1–M3.

## 3. Изменённые репозитории и файлы

### `integration-platform`

| Область | Файлы | Результат |
| --- | --- | --- |
| Delivery domain | `backend/src/delivery_service/delivery/models.py`, `application.py`, `ports.py` | Новый aggregate, DeliveryAttempt, state methods, retry processor и local ports. |
| Worker | `backend/src/delivery_service/main.py`, `configuration.py` | Local poller для queued/retry Delivery и stale recovery; временная ошибка старта или миграции не убивает worker. |
| Processing boundary | `backend/src/integration_api/message_processing/processing.py`, `delivery_completion.py` | Успешный transform snapshot-ит target и создаёт Delivery; Message получает итог по состояниям Delivery. |
| HTTP transport | `backend/src/integration_api/message_processing/http_delivery.py` | `HttpDeliveryExecutor`, structured error classification, immutable target URL и `Idempotency-Key` для трёх target-систем. |
| Wiring и settings | `backend/src/integration_api/main.py`, `configuration/settings.py` | Retry settings и немедленный local poll для только что созданных Delivery. |
| Миграции | `backend/migrations/versions/20261009_0004_delivery_runtime.py` | Таблицы Delivery/Attempt, ограничения и индексы. |
| Совместимость миграций | `backend/migrations/versions/20261009_0003_route_result_per_route.py` | M3 unique constraint переведён на Alembic batch mode: clean SQLite migration test теперь работает так же, как PostgreSQL. |
| Тесты | `backend/tests/test_delivery_runtime.py` и обновлённые event, processing, Warehouse, health tests | Детальное покрытие M4 и отказ от нестабильного Windows `TestClient` harness. |
| Документация | `README.md`, `docs/README.md`, данный файл | Описание текущего runtime и M4. |

### `integration-environment`

В `docker-compose.yml` добавлены статические retry settings для
`integration-api`, а также timeout, poll interval и stale threshold для
`delivery-service`.

В `demo-business-systems` и `warehouse-1c` M4 source changes нет.

## 4. Persistence-модель и миграция

Alembic revision `20261009_0004` создаёт две Platform-owned таблицы:

| Таблица | Назначение | Основные ограничения |
| --- | --- | --- |
| `deliveries` | Одна логическая outbound delivery с immutable transport/retry snapshot. | `UNIQUE(route_processing_result_id)`, `attempt_count >= 0`, `max_attempts >= 1`, `lock_version`. |
| `delivery_attempts` | История каждого фактического transport execution. | `UNIQUE(delivery_id, attempt_number)`, FK только внутри Delivery Context. |

`message_id` и `route_processing_result_id` — логические cross-context
references. Между Message Processing и Delivery нет физического foreign key.

### Поля `deliveries`

| Группа | Persisted fields |
| --- | --- |
| Identity и source | `id`, `message_id`, `route_processing_result_id`, `route_key`, `route_version`, `target_system` |
| Immutable target snapshot | `payload_format`, `transformed_payload`, `target_url`, `http_method` |
| Lifecycle | `status`, `attempt_count`, `next_retry_at`, `processing_started_at`, `completed_at`, `lock_version` |
| Retry snapshot | `max_attempts`, `retry_strategy`, `retry_initial_delay_seconds`, `retry_max_delay_seconds`, `retry_multiplier`, `retryable_errors` |
| Error / dead letter | `last_error_category`, `last_error_code`, `last_error_message`, `dead_letter_reason` |
| Audit и idempotency | `created_at`, `updated_at`, `idempotency_key` |

### Поля `delivery_attempts`

`id`, `delivery_id`, `attempt_number`, `started_at`, `completed_at`, `outcome`,
`http_status`, `error_category`, `error_code`, `error_message`, `created_at`.

Payload в Attempt не копируется: все попытки одной Delivery используют один
immutable parent snapshot.

### Ограничения и индексы

- `UNIQUE(deliveries.route_processing_result_id)` защищает от двух logical
  Delivery для одного успешного route processing result;
- `CHECK(attempt_count >= 0)` и `CHECK(max_attempts >= 1)` защищают базовые
  aggregate invariants;
- `UNIQUE(delivery_attempts.delivery_id, delivery_attempts.attempt_number)`
  не допускает gaps и дубли attempt number;
- индексы созданы для `message_id`, `target_system/status`, due retry,
  stale processing и history lookup;
- `delivery_attempts.delivery_id -> deliveries.id` — единственный physical FK.

Миграция применена на живом PostgreSQL. Проверка показала:

```text
deliveries
delivery_attempts
alembic_version = 20261009_0004
```

## 5. Delivery aggregate и state machine

Строго используются статусы:

```text
PENDING -> QUEUED -> PROCESSING -> DELIVERED
                         |
                         +-> RETRY_WAIT -> QUEUED
                         |
                         +-> DEAD_LETTER
```

Terminal states: `DELIVERED`, `DEAD_LETTER`.

Domain methods: `queue()`, `start_processing()`, `mark_delivered()`,
`register_failure()`, `schedule_retry()`, `dead_letter()`.

`attempt_count` увеличивается только при `QUEUED -> PROCESSING`. В той же
транзакции создаётся `DeliveryAttempt` с `attempt_number = attempt_count`.
Автоматический processor и stale recovery не могут оживить terminal Delivery.

## 6. Retry, error classification и stale recovery

Статические M4 settings snapshot-ятся в Delivery в момент создания:

| Setting | Значение по умолчанию |
| --- | --- |
| `DELIVERY_MAX_ATTEMPTS` | `3` |
| `DELIVERY_RETRY_STRATEGY` | `exponential` |
| `DELIVERY_RETRY_INITIAL_DELAY_SECONDS` | `5` |
| `DELIVERY_RETRY_MAX_DELAY_SECONDS` | `300` |
| `DELIVERY_RETRY_MULTIPLIER` | `2` |
| `DELIVERY_STALE_PROCESSING_TIMEOUT_SECONDS` | `60` |

Backoff для attempt `n`:

```text
min(initial_delay * multiplier^(n - 1), max_delay)
```

| Transport outcome | Результат Delivery |
| --- | --- |
| Timeout | `RETRY_WAIT` |
| Connection error | `RETRY_WAIT` |
| HTTP 503 и другой 5xx | `RETRY_WAIT` |
| HTTP 429 | `RETRY_WAIT` |
| Deterministic 4xx, включая Mini Shop 409 | `DEAD_LETTER` |
| Retryable failure на последней попытке | `DEAD_LETTER` |
| CRM/Mini Shop 2xx, Warehouse 202 | `DELIVERED` |

Stale recovery выбирает `PROCESSING` Delivery, у которых
`processing_started_at` старше threshold. Текущий Attempt сохраняется,
помечается `INTERRUPTED` с `STALE_PROCESSING`, failure фиксируется структурно,
после чего Delivery получает `RETRY_WAIT` (либо `DEAD_LETTER`, если лимит уже
исчерпан). Stale Delivery никогда не становится `DELIVERED` без HTTP result.

`warehouse.reservation.failed` не является transport failure. Это корректное
бизнес-событие, доставляемое в Mini Shop; исходную Warehouse Delivery повторять
из-за `OUT_OF_STOCK` нельзя.

## 7. Idempotency, locking и target adapters

Для original Delivery:

```text
delivery.idempotency_key = delivery.id
```

Каждая retry attempt использует тот же key. Future manual requeue в M4 не
реализован; позднее он создаст новую Delivery, сохранив original logical key.

Processor выбирает `QUEUED` и due `RETRY_WAIT`. Claim выполняет
`QUEUED -> PROCESSING`, меняет `lock_version` и создаёт Attempt в одной
транзакции. SQLAlchemy versioning не позволит двум worker с одной старой
версией одновременно успешно сделать claim.

`HttpDeliveryExecutor` использует сохранённые `target_url`, `http_method`,
`payload_format` и `transformed_payload`, а не заново читает Route:

| Target | Success rule | Endpoint / payload |
| --- | --- | --- |
| CRM | любой `2xx` | прежний JSON `POST /api/integration/orders` |
| Warehouse 1С | ровно `202` | прежний XML `POST /hs/reservations/` |
| Mini Shop | любой `2xx` | прежний JSON `POST /api/integration/reservation-results` |

Все три target-системы получают `Idempotency-Key`, включая Warehouse 1С. В
Delivery не сохраняются секреты.

## 8. Message completion semantics

Пока необходимая Delivery активна (`PENDING`, `QUEUED`, `PROCESSING` или
`RETRY_WAIT`), Message остаётся `VALIDATED` и не считается успешной.

- все required Delivery = `DELIVERED` -> `Message = COMPLETED`;
- вся работа terminal и хотя бы одна Delivery = `DEAD_LETTER`, либо processing
  завершился ошибкой -> `Message = ROUTING_FAILED`;
- `NO_ROUTE` и `VALIDATION_FAILED` остаются без изменений.

`PARTIAL_SUCCESS` как Message domain status не добавлен. В будущем его можно
вычислять в отдельной read model из individual Delivery states.

## 9. Проверки

Полный backend test suite:

```text
53 passed in 1.67s
```

Покрыты:

- valid ingestion, source validation, source-event idempotency и no-route;
- clean migration upgrade до `20261009_0004`;
- разрешённые и запрещённые Delivery transitions;
- first/retry attempt numbering, immutable snapshots и неизменный
  idempotency key;
- timeout и HTTP 503 -> `RETRY_WAIT`, due retry -> success;
- non-retryable failure и Mini Shop HTTP 409 -> `DEAD_LETTER`;
- retry exhaustion; stale `PROCESSING` -> `INTERRUPTED` + retry;
- optimistic locking при concurrent claim;
- fan-out isolation: CRM Delivery не повторяется, когда Warehouse неуспешен;
- M2 Warehouse XML и M3 Warehouse callback transformation/delivery.

Предыдущий full-suite hang был изолирован в Windows execution FastAPI
`TestClient`/AnyIO. Эти tests переписаны как deterministic direct
application-service tests. Это не убирает HTTP confidence: реальный HTTP путь
и все три adapter-а пройдены в Docker E2E ниже.

## 10. Живой business E2E

После deployment M4 создан обычный Mini Shop order:

```text
order_id: 4b26c4d0-3344-45cc-bf11-65f329918e1f
initial order status: WAITING_FOR_RESERVATION
```

Inbound `order.created` Message получил `COMPLETED` после двух независимых
Delivery:

| Target | Delivery ID | Attempt | HTTP | Result |
| --- | --- | --- | --- | --- |
| CRM | `5b193caa-f1b7-4048-a0b3-36e3d584d15b` | 1 | 200 | `DELIVERED` |
| Warehouse 1С | `38ad1415-e057-45d0-9e25-400bfb438f26` | 1 | 202 | `DELIVERED` |

Для обеих записей persisted `idempotency_key` совпал с Delivery ID.

Затем 1С отправила реальный callback:

```text
EventId: 96ce5b68-2f92-4851-9ac4-32925e576014
EventType: reservation.succeeded
```

Platform сохранила callback Message
`84d0bebe-5bcd-42f0-97c8-9338bbdb2a7f` со статусом `COMPLETED`, создала Mini
Shop Delivery `163779ef-4d88-4847-b2c8-dfbde1293571`, доставила её с первой
попытки (`HTTP 200`, `DELIVERED`). В Mini Shop итоговый order status:

```text
CONFIRMED
```

### Проверка business-failure path

Дополнительно создан order с заведомо недостаточным остатком:

```text
order_id: bc042ce8-40fd-4def-9e20-cf74506e6c49
item: SKU-002, quantity: 10
```

Исходные CRM и Warehouse Delivery снова получили `DELIVERED` с первой попытки
(`HTTP 200` и `HTTP 202` соответственно): отсутствие товара — не transport
ошибка доставки ReservationRequest.

Warehouse callback:

```text
EventId: f37f5917-cd5d-48d5-addc-ebfc1a56dc33
EventType: reservation.failed
Reason: OUT_OF_STOCK
Message: Insufficient stock for product Second Product. Available: 1, requested: 10
```

Platform сохранила callback Message
`e18a14aa-2bac-4980-a261-512938c81946` как `COMPLETED`, создала Mini Shop
Delivery `2ce8322b-a228-4aea-88cf-c492032ad04d`; первая попытка завершилась
`HTTP 200` и `DELIVERED`. Итоговый status в Mini Shop:

```text
REJECTED
```

Тем самым M4 подтверждён для обоих бизнес-результатов Warehouse: успешного
резервирования и бизнесового отказа, которые не смешиваются с transport retry.

## 11. Ограничения, сознательно оставленные после M4

Следующие части не реализованы, потому что относятся к последующим milestone:

- Kafka runtime для Delivery;
- Processing Outbox;
- Delivery Inbox/consumer;
- persistent/versioned RetryPolicy;
- manual requeue;
- Configuration Context и `FIELD_MAPPING_V1`;
- operational query API, Admin UI и metrics/dashboard.

M4 не создаёт преждевременно Kafka Outbox/Inbox и не добавляет
`PARTIAL_SUCCESS` в Message domain model.
