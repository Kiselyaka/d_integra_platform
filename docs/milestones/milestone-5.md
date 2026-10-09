# Milestone 5 — Kafka, transactional Outbox и Inbox

## 1. Цель

M5 заменяет временную локальную границу M4 между Message Processing и Delivery
на асинхронную, устойчивую к повторной доставке границу через Kafka.

Внешние HTTP-контракты и ownership не меняются: Mini Shop по-прежнему создаёт
`order.created`, Platform вызывает CRM и Warehouse 1С, а 1С присылает свой
callback в Platform. Apache/Laragon и публикация 1С остаются на Windows-хосте;
контейнер обращается к ним по `host.docker.internal:8081`.

```text
Message Processing                                  Delivery
------------------                                  --------
Message + Processing Outbox --Kafka requested-->    Delivery Inbox + Delivery
                                                           |
                                                           v
Message Inbox <--Kafka terminal-- Delivery Outbox + terminal Delivery
```

Гарантия между этими компонентами — **at-least-once**, а не exactly-once.
Повторная доставка Kafka компенсируется Inbox и ограничениями базы данных.

## 2. Что изменилось относительно M4

В M4 `EventProcessingService` вызывал local
`DeliveryRequestService`, а `DeliveryProcessor` напрямую читал Delivery для
пересчёта статуса Message. В M5 обе прямые связи удалены из production path:

```text
M4: Processing -> DeliveryRequestService -> Delivery
    Delivery -> MessageDeliveryCompletionService -> SELECT deliveries

M5: Processing transaction -> Processing Outbox -> Kafka -> Delivery Inbox
    Delivery terminal transaction -> Delivery Outbox -> Kafka -> Processing Inbox
```

`DeliveryRequestService` сохранен только как изолированный compatibility seam
старых unit-тестов; runtime его не вызывает. Удалён
`message_processing/delivery_completion.py` — Processing больше не читает
таблицу Delivery.

## 3. События и consumer groups

| Topic | Event | Publisher | Consumer group | Consumer |
| --- | --- | --- | --- | --- |
| `integration.delivery.requested` | `delivery.requested` | Processing Outbox | `delivery-workers` | Delivery service |
| `integration.delivery.completed` | `delivery.completed` | Delivery Outbox | `message-completion-handler` | Integration API |
| `integration.delivery.dead_lettered` | `delivery.dead_lettered` | Delivery Outbox | `message-completion-handler` | Integration API |

Все internal events используют общий versioned envelope:

```json
{
  "event_id": "uuid",
  "event_type": "delivery.requested",
  "aggregate_id": "uuid",
  "occurred_at": "2026-10-09T18:19:59Z",
  "correlation_id": "order-...",
  "schema_version": "1",
  "payload": {}
}
```

`delivery.requested` содержит immutable snapshot route, target URL/method,
преобразованного payload и retry policy. Поэтому Delivery consumer не читает
Route и не зависит от последующего изменения конфигурации.

## 4. Transactional Outbox

### Processing Outbox

В одной транзакции Processing создаёт:

1. `RouteProcessingResult` в `DELIVERY_REQUESTED`;
2. одну запись `processing_outbox_events` для каждого matching route.

HTTP endpoint при этом не ждёт Kafka. Poller забирает неопубликованные записи
с lease (`publish_lease_token`, `publish_lease_until`), отправляет их с
Kafka-key, равным `delivery_id`, и помечает `published_at` только после broker
acknowledgement. Падение между broker ACK и обновлением строки может повторить
event — это ожидаемый at-least-once сценарий.

### Delivery Outbox

Когда Delivery переходит в terminal state, в той же транзакции сохраняются:

- `DeliveryAttempt` и новый state Delivery;
- при `DELIVERED` — `delivery.completed` в `delivery_outbox_events`;
- при `DEAD_LETTER` — `delivery.completed` со status `DEAD_LETTER` **и**
  отдельный `delivery.dead_lettered` с причиной terminal failure.

Уникальность `(delivery_id, event_type)` исключает второй terminal event того
же вида для одной Delivery.

## 5. Inbox, идемпотентность и Message completion

### Delivery Inbox

После получения `delivery.requested` consumer в одной БД-транзакции сохраняет
Inbox marker и создаёт Delivery с ID, уже сгенерированным Processing:

```text
delivery.id = payload.delivery_id
delivery.idempotency_key = payload.delivery_id
```

Повтор того же Kafka event не создаст вторую Delivery. Помимо Inbox применяется
ограничение существующего `route_processing_result_id`.

### Processing Inbox

`delivery.completed` и `delivery.dead_lettered` потребляются одной consumer
group и сохраняются как Inbox marker и `ProcessingDeliveryCompletion`.
`delivery.dead_lettered` нормализуется в terminal status `DEAD_LETTER`.
Completion service больше не выполняет `SELECT` из `deliveries`:

- есть ещё незавершённые route results -> Message остаётся `VALIDATED`;
- все required route results получили `DELIVERED` -> `COMPLETED`;
- вся работа terminal, но есть `DEAD_LETTER` -> `ROUTING_FAILED`.

Уникальность `(consumer_name, event_id)` на обеих Inbox-таблицах делает
повторные Kafka deliveries безопасными. Offset коммитится только после
успешного DB handler.

## 6. Persistence и миграция

Миграция `20261009_0005_kafka_outbox_inbox.py` добавляет пять
Platform-owned таблиц:

| Таблица | Назначение |
| --- | --- |
| `processing_outbox_events` | надёжное намерение Processing запросить Delivery |
| `delivery_inbox_messages` | dedup `delivery-workers` |
| `delivery_outbox_events` | terminal result Delivery для Kafka |
| `processing_inbox_messages` | dedup `message-completion-handler` |
| `processing_delivery_completions` | локальная projection terminal Delivery для Message |

Также в `deliveries` добавлен `correlation_id`. Миграция применена к локальной
`integration_db` при старте `integration-api`:

```text
Running upgrade 20261009_0004 -> 20261009_0005
```

## 7. Docker runtime

`integration-environment/docker-compose.yml` теперь поднимает Kafka и
одноразовый `kafka-init`. После health-check брокера он создаёт три topic через
`kafka-topics.sh --create --if-not-exists`; запуск повторяем и безопасен.

`integration-api` запускает Processing Outbox publisher и completion consumer.
`delivery-service` запускает requested consumer, существующий M4 retry/stale
poller и Delivery Outbox publisher. Kafka brokers доступны контейнерам по
`kafka:9092`.

Для нестабильного внешнего соединения Dockerfile API и Delivery устанавливают
Python packages с увеличенными `pip` timeout/retry. Это изменение сборки, не
часть доменной логики M5.

## 8. Неизменённые части

M5 не меняет:

- XML `ReservationRequest` и XML callbacks Warehouse 1С;
- HTTP paths Mini Shop, CRM и Warehouse;
- M4 Delivery aggregate, retry policy, attempts, stale recovery,
  `Idempotency-Key` и HTTP error classification;
- конфигурацию 1С и Apache/Laragon;
- бизнесовые статусы `CONFIRMED` / `REJECTED` Mini Shop.

Бизнесовый `reservation.failed` остаётся нормальным callback-событием, а не
Kafka или transport error. Он доставляется в Mini Shop через тот же M5 path.

## 9. Проверки

Полный backend suite:

```text
61 passed
```

В частности покрыты fan-out Outbox, Delivery Inbox deduplication, сохранение
предварительно сгенерированного Delivery ID, terminal Outbox, completion
projection для `completed` и `dead_lettered`, duplicate terminal event и повторная публикация после падения
между Kafka ACK и `published_at`.

### Живой Docker E2E

После запуска Kafka и сервисов создан Mini Shop order:

```text
order_id: 44a13622-9e4e-410d-a257-aa08dbb3cfce
item: SKU-001, quantity: 1
```

Наблюдаемый M5 flow:

| Этап | Подтверждение |
| --- | --- |
| Processing Outbox | два `delivery.requested`, оба `published_at`, одна publish attempt |
| Delivery Inbox | два `delivery-workers` Inbox markers `PROCESSED` |
| Delivery | CRM и Warehouse 1С: `DELIVERED`, attempt `1` |
| Delivery Outbox | два `delivery.completed`, оба опубликованы |
| Processing Inbox | два `message-completion-handler` markers `PROCESSED` |
| Message | `order.created = COMPLETED`, две `DELIVERED` completion projections |

Warehouse 1С затем отправил фактический callback:

```text
EventId: 2fb835c1-1c4d-4461-9040-58a5cd8fa84c
AggregateId: 44a13622-9e4e-410d-a257-aa08dbb3cfce
EventType: warehouse.reservation.succeeded
```

Platform обработала callback как `COMPLETED`, Mini Shop Delivery получила
`DELIVERED` с первой попытки, и заказ стал `CONFIRMED`. Это подтверждает, что
новая M5 messaging boundary работает и для обратного Warehouse business flow.

### Живые fault-injection проверки

После основного E2E дополнительно выполнены сценарии, которые нельзя увидеть
только по успешному заказу:

| Сценарий | Проверяемое свойство | Результат |
| --- | --- | --- |
| Kafka остановлена до нового order | Processing Outbox сохраняет намерение без Kafka | Заказ `dcba4217-fbbf-4809-8dbb-1ca0dcdb1975` принят (`201`); две записи `delivery.requested` остались неопубликованными, Delivery не создана. После запуска Kafka обе записи опубликованы, обе Delivery стали `DELIVERED`, Message — `COMPLETED`. |
| Delivery Service остановлен | Kafka хранит событие до возврата consumer | Заказ `895c2975-70e9-4504-9dea-115471d980df` создал две published requested events; `delivery-workers` не имел active member и lag вырос. После запуска сервиса lag стал `0`, обе Delivery стали `DELIVERED`, Message — `COMPLETED`. |
| Повтор `delivery.requested` | Delivery Inbox deduplication | Processing Outbox повторно опубликовал тот же `event_id`; `publish_attempt_count = 2`, но Inbox marker остался один, а число Delivery для заказа — две. Второго HTTP-вызова Warehouse не создано. |
| Повтор `delivery.completed` | Processing Inbox deduplication | Delivery Outbox повторно опубликовал тот же terminal event; `publish_attempt_count = 2`, Inbox marker остался один, Message сохранил `COMPLETED`. |
| Mini Shop 404 | Terminal failure path | Искусственный callback для несуществующего order создал Delivery `DEAD_LETTER` с `HTTP_404`, Message стал `ROUTING_FAILED`; оба события `delivery.completed` и `delivery.dead_lettered` опубликованы и обработаны Processing Inbox. |

Повторный callback `ReservationSucceeded` для уже `CONFIRMED` заказа также
проверен: Mini Shop идемпотентно вернул `200`, Delivery осталась `DELIVERED`,
а бизнес-статус заказа не изменился.

## 10. Ограничения после M5

- Kafka работает как single-broker local environment; production clustering,
  ACL, encryption и monitoring не входят в milestone.
- Ручной requeue, operational dashboard и отдельный Kafka DLQ для
  malformed/poison internal records остаются следующими задачами.
  `integration.delivery.dead_lettered` уже реализован: это domain/operational
  event о terminal `Delivery = DEAD_LETTER`, а не очередь необрабатываемых
  Kafka-records.
- Outbox publisher использует polling и lease; это намеренная простая local
  реализация, а не CDC.

## 11. M5.5 — Kafka Runtime Visualization

M5.5 добавляет `kafka-ui` как отдельный optional Compose service для локальной
диагностики. Это development tooling, а не capability Platform:

```text
Kafka UI -> Kafka
           development/debugging only
```

Сервис запускается только явной командой:

```sh
docker compose --profile tooling up -d kafka-ui
```

Он доступен лишь с Windows-хоста по `http://localhost:8085`, подключается к
`kafka:9092` в read-only mode и позволяет посмотреть три M5 topic, messages,
partitions, consumer groups, offsets и lag. Основные сервисы не имеют
`depends_on: kafka-ui`, поэтому его отсутствие, остановка или удаление не
влияют на Platform.

Kafka UI не является source of truth, не обращается к Platform domain API, не
строит Message/Delivery/Outbox/Inbox projections и не используется для
business operations. Позже его заменят Platform read models через M9
Operational Query API → M10 React Admin UI и M11 observability; контейнер
можно будет оставить инженерным инструментом или удалить без изменения
runtime architecture.
