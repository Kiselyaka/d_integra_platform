# Milestone 7 — FIELD_MAPPING_V1 Transformation Engine

## Цель

M6 сделал data-driven маршрутизацию. M7 делает data-driven само преобразование: demo-маршрут получает `TransformationDefinition` из PostgreSQL и исполняет её generic engine, а не Python handler для CRM, Warehouse или Mini Shop.

```text
Message + IntegrationRoute
→ TransformationDefinition (FIELD_MAPPING_V1, definition JSON)
→ typed AST + validator
→ FieldMappingV1Engine
→ canonical target document
→ JSON/XML serializer
→ delivery.requested snapshot
```

Engine не знает названия внешних систем, event type или business fields. Он не делает HTTP, SQL, filesystem/environment access, `eval`, Python/JS execution или enrichment.

## Точный DSL

Корень каждого definition:

```json
{"target":{"field":{"op":"from","path":"$.source_field"}}}
```

Операции всегда используют явный `op`; поле target с именем `from` не становится случайно expression.

| Операция | Форма |
| --- | --- |
| source path | `{"op":"from","path":"$.customer.email"}` |
| literal | `{"op":"literal","value":"SUCCEEDED"}` |
| default | `{"op":"default","value":<expr>,"fallback":<expr>}` |
| concat | `{"op":"concat","values":[<expr>,...]}` |
| map | `{"op":"map","path":"$.items","as":"item","target":<node>}` |
| conversion | `{"op":"to_string|to_integer|to_decimal|to_boolean","value":<expr>}` |
| condition | `{"op":"if","condition":<condition>,"then":<node>,"else":<node>}` |
| omit | `{"op":"omit"}` |

Conditions: `exists(path)`, `equals(left,right)`, `not_equals(left,right)`, `and(values)`, `or(values)`, `not(value)`. Они short-circuit слева направо.

`$` — весь canonical source payload; `$item` — переменная текущего `map`; `$event.event_id` — контролируемый context metadata. Полный JSONPath, filters, wildcards, predicates и functions не поддерживаются. Nested map сохраняет root и outer variables; одинаковое имя variable затеняет outer variable только внутри nested map.

`from` для отсутствующего path выдаёт `SOURCE_PATH_NOT_FOUND`; существующий path со значением `null` возвращает `null`. `default` использует fallback для missing/null, но оставляет `""`, `0` и `false` как есть. `exists` возвращает false для missing и null. `omit` удаляет property object, а не создаёт `null`; как array/map item он запрещён.

Conversions намеренно строгие: `to_integer` не отбрасывает дробную часть; `to_decimal` использует `Decimal`; `to_boolean` принимает только boolean либо точные строки `"true"` / `"false"`; `to_string` не stringify-ит object, array или null. `concat` также не принимает null/object/array.

## Demo definitions

В `configuration.transformation_definitions.definition` лежат четыре definition с `engine_type = FIELD_MAPPING_V1`:

- `order-created-to-crm`;
- `order-created-to-warehouse-reservation`;
- `warehouse-reservation-succeeded-to-mini-shop`;
- `warehouse-reservation-failed-to-mini-shop`.

Warehouse definition строит ordered canonical XML document (`ReservationRequest` / `OrderNumber` / `Positions` / `Position`) через JSON array `children`. Это необходимо, потому что PostgreSQL JSONB не сохраняет порядок ключей object. Generic XML serializer превращает этот нейтральный document в опубликованный XML 1С. DSL не содержит XML string, а engine не содержит Warehouse-specific branch.

## Ошибки

| Ситуация | RouteProcessingResult |
| --- | --- |
| malformed/unknown DSL operation, bad path, invalid AST | `CONFIGURATION_ERROR` |
| valid AST, но missing source path/type/conversion failure | `TRANSFORMATION_FAILED` |

Ошибка хранится структурированно: `category`, `message`, `operation`, `source_path`, `target_path`, `details`. Для failed route не создаются `delivery.requested` и Delivery; вторая fan-out ветка продолжает работу.

## Обновление M6 базы

Миграции `20261010_0007` и corrective `20261010_0008` сохраняют `id`, `transformation_key` и `version`. Они обновляют только известные исходные M6 demo rows, если у них `engine_type = BUILTIN` и definition пуста (SQL `NULL` либо JSON `null`, которым M6 сохранил Python `None`). `20261010_0009` меняет только exact early-M7 Warehouse definition на ordered form; operator-modified definition не затрагивается. Маршруты, retry policy, Messages, RouteProcessingResults, Delivery, Outbox и Inbox не удаляются и не пересоздаются.

Исторические Delivery уже содержат immutable transformed payload, поэтому смена engine не меняет их повторную отправку. BUILTIN код остаётся лишь для исторических M1--M6 focused fixtures; current seeded demo routes на него не ссылаются.

## Границы M7

M7 не реализует M8: нет source/target schema validation pipeline, preview, DRAFT/ACTIVE/RETIRED lifecycle, activation workflow, Admin UI, arbitrary scripting или external enrichment.

## Проверка реализации

Автоматические проверки: `113 passed`.

Покрыты AST/path semantics, missing vs null, default, concat, map и nested map,
conversions, condition short-circuit, omit, unsafe/unknown operations, structured
error mapping, fan-out independence, data-driven definition mutation, JSON/XML
parity с M6 BUILTIN fixtures и snapshot invariant.

Живое M7 E2E в локальном Docker/1С окружении:

| Сценарий | Доказательство |
| --- | --- |
| success | Order `04999d95-ffed-4329-bd17-ea82f5cfb995`, `SKU-002 × 1`; CRM и Warehouse Delivery `DELIVERED`; callback Message `4af8aa6f-c3c4-4ea5-b341-130ff5b365e1` — `warehouse.reservation.succeeded`; Mini Shop `CONFIRMED`. Warehouse snapshot содержит `ReservationRequest → OrderNumber → Positions` в опубликованном порядке. |
| business failure | Order `8e7a484e-65f7-4f21-bd49-f0c6ab9ec927`, `SKU-001 × 1`; Warehouse принял запрос, но callback вернул `OUT_OF_STOCK`; callback route доставлен в Mini Shop; итог `REJECTED`. |

В живой `integration_db` все четыре definition имеют `FIELD_MAPPING_V1` и JSON
object definition. Runtime retry policy, изменённая во время M6, сохранена:
5 попыток с паузами 15/30/60/120 секунд. M7 не меняет Kafka/Outbox/Inbox и не
перевыполняет transformation во время retry Delivery.

Fresh-install также проверен в изолированной временной PostgreSQL БД: `alembic
upgrade head` с нуля и стандартный seed дали `4/4` FIELD_MAPPING_V1 JSON
definition. После проверки временная БД удалена; рабочая `integration_db` не
сбрасывалась.
