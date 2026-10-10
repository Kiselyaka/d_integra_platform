# Milestone 8 — Validation, Preview and Versioning

## Цель

M8 делает configuration безопасной для изменения: контракт проверяется до
создания Delivery, preview использует production pipeline без побочных эффектов,
а боевые версии configuration становятся неизменяемыми.

## Preprocessing pipeline

```text
intake / technical envelope checks
→ route lookup (ACTIVE + enabled)
→ source contract validation
→ FIELD_MAPPING_V1
→ serialization / target contract validation
→ RouteProcessingResult + Processing Outbox
```

Для JSON target JSON Schema проверяется перед сериализацией. Для XML target
сначала создаётся реальный XML, затем именно он проверяется Warehouse XSD.

Для Warehouse callback raw XML bytes передаются transient input в pipeline и
проверяются callback XSD до business mapping. Raw XML не архивируется в Message.

## Contracts and outcomes

| Ситуация | RouteProcessingResult | Delivery / Outbox |
| --- | --- | --- |
| Source contract не совпал | `SOURCE_VALIDATION_FAILED` | Не создаются |
| FIELD_MAPPING_V1 не смог преобразовать данные | `TRANSFORMATION_FAILED` | Не создаются |
| Target JSON/XSD не совпал | `TARGET_VALIDATION_FAILED` | Не создаются |
| Некорректная/inactive configuration | `CONFIGURATION_ERROR` | Не создаются |
| Все стадии успешны | `DELIVERY_REQUESTED` | Создаются одной транзакцией |

Fan-out независим: ошибка CRM route не отменяет Warehouse route и наоборот.

## Lifecycle

`ValidationSchema`, `TransformationDefinition` и `IntegrationRoute` имеют
статусы `DRAFT`, `ACTIVE`, `RETIRED`.

```text
DRAFT → ACTIVE → RETIRED
```

Изменять разрешено только `DRAFT`. Изменение active/retired версии выполняется
через создание следующей версии. При activation replacement текущая ACTIVE
версия того же key становится RETIRED в одной транзакции. PostgreSQL partial
unique indexes защищают от двух ACTIVE versions одного key.

`enabled` у route остаётся operational kill-switch; runtime condition —
`status=ACTIVE AND enabled=true`.

## Preview

- `POST /api/v1/routes/{id}/preview` — полная route pipeline;
- `POST /api/v1/transformations/{id}/preview` — mapping/serialization;
- lifecycle commands находятся под `/api/v1/configuration/...`.

Preview вызывает тот же `PreprocessingPipeline`, но не создаёт Message, RPR,
Delivery, Outbox/Inbox/Kafka events и не выполняет HTTP-запросы к целевым
системам.

## Demo contracts

Seed создаёт шесть ACTIVE schemas: Mini Shop `order.created` source, CRM target,
Mini Shop reservation-result target, Warehouse ReservationRequest XSD и XSD для
Warehouse success/failure callback. JSON Schema external `$ref` и XSD
include/import запрещены: schemas self-contained и не могут загружать сеть/файлы.

## Проверка реализации

- Полный test suite: `117 passed`.
- Fresh PostgreSQL database: все миграции применяются с нуля до
  `20261010_0010`; штатный seed создаёт 6 ACTIVE schemas, 4 ACTIVE routes и
  4 ACTIVE `FIELD_MAPPING_V1` transformations.
- Lifecycle: новый route/schema version создаётся как `DRAFT`; activation
  retirement/activation выполняет в одной транзакции в корректном порядке.
  При одновременной activation двух DRAFT transformations один запрос стал
  `ACTIVE`, второй получил `ConfigurationConflictError`; итоговое состояние
  `v1=RETIRED`, `v2=ACTIVE`, `v3=DRAFT`. PostgreSQL partial unique index
  отклонил намеренную попытку создать вторую ACTIVE transformation того же key.
- Preview: Warehouse route вернул валидный `ReservationRequest` без создания
  runtime Message/RPR/Processing Outbox/Delivery/Delivery Outbox/Inbox/attempt
  и без HTTP-вызова в 1С или Kafka event.
- Отдельные acceptance tests подтверждают `SOURCE_VALIDATION_FAILED`,
  `TRANSFORMATION_FAILED` и `TARGET_VALIDATION_FAILED`; для XML target
  специально проверен неверный порядок `Positions` и `OrderNumber` — XSD
  отклоняет уже сериализованный XML с `XSD_VALIDATION_FAILED`.
- Live E2E через Mini Shop, Platform, CRM и опубликованную 1С:
  - `2c9f70b3-2b69-48c9-8408-9781b6ac2ad1`, `SKU-001 × 1`:
    Warehouse callback `reservation.failed` / `OUT_OF_STOCK`, Mini Shop
    `REJECTED`;
  - `85535d23-4162-4797-9c70-122467524321`, `SKU-002 × 1`:
    Warehouse callback `reservation.succeeded`, Mini Shop `CONFIRMED`.

В обоих E2E `order.created` создал независимые CRM и Warehouse route results,
а callback 1С был принят и доставлен в Mini Shop по M8 validation pipeline.
