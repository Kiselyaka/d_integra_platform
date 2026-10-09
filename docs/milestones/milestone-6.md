# Milestone 6 — Configuration Context

## Результат

M6 перенёс выбор маршрутов из Python-условий в постоянную конфигурацию PostgreSQL. Перед созданием `RouteProcessingResult` и записи в Processing Outbox Message Processing запрашивает подходящие маршруты у Configuration Context. Delivery остаётся независимым: он получает самодостаточный snapshot `delivery.requested` и больше не читает конфигурацию.

## Владение и хранение данных

Configuration Context владеет PostgreSQL-схемой `configuration`:

- `auth_profiles` — метаданные и ссылки на секреты; сами секреты не хранятся;
- `external_systems` — машинный код системы, базовый URL, состояние, ссылка на auth profile и timeout;
- `validation_schemas` — сохранённые ссылки для M8, но не validation engine в M6;
- `transformation_definitions` — постоянная identity преобразования;
- `retry_policies` — параметры повторов и перечень повторяемых ошибок;
- `integration_routes` — правило source/event → target, endpoint, форматы, transformation и retry policy.

Физические внешние ключи существуют только внутри схемы `configuration`. Runtime-контексты хранят логические исторические идентификаторы и не получают физических FK на конфигурацию. Миграция `20261010_0006_configuration_context` сохраняет все существующие M1–M5 записи.

В `RouteProcessingResult` теперь сохраняются использованные `route_id`/версия, `transformation_id`/версия и, при наличии, identity validation schema. У исторических записей до M6 эти новые поля корректно остаются `NULL`.

## Demo bootstrap

Команда `python -m integration_api.configuration.seed` добавляет только отсутствующие записи со стабильными ключами и не перезаписывает изменения оператора. Контейнер API запускает её после `alembic upgrade head`.

| Источник | Событие | Маршрут | Получатель | Преобразование |
| --- | --- | --- | --- | --- |
| `mini-shop` | `order.created` | `mini-shop-order-created-to-crm` | `crm` | `order-created-to-crm` |
| `mini-shop` | `order.created` | `mini-shop-order-created-to-warehouse` | `1c-warehouse` | `order-created-to-warehouse-reservation` |
| `1c-warehouse` | `warehouse.reservation.succeeded` | `warehouse-reservation-succeeded-to-mini-shop` | `mini-shop` | `warehouse-reservation-succeeded-to-mini-shop` |
| `1c-warehouse` | `warehouse.reservation.failed` | `warehouse-reservation-failed-to-mini-shop` | `mini-shop` | `warehouse-reservation-failed-to-mini-shop` |

XML wire contract 1С не менялся. M6 только нормализовал внутренний код источника Platform: прежний `warehouse-1c` заменён на `1c-warehouse`. Для upgrade compatibility `PostgresRouteConfigurationProvider`, source validation и callback transformations принимают оба source code. Кроме того, Delivery executor считает как исторический `target_system = warehouse`, так и новый `target_system = 1c-warehouse` Warehouse-target с успешным ответом HTTP `202`.

`CRM_BASE_URL`, `WAREHOUSE_BASE_URL`, `MINI_SHOP_BASE_URL` и retry settings стали только baseline bootstrap defaults для чистой БД. Во время работы Platform читает уже сохранённую конфигурацию PostgreSQL. Seed работает по принципу insert-if-missing: намеренно удалённая demo-запись будет создана снова при restart API. Это допустимо только для local demo; в production seed должен запускаться отдельной явно управляемой операцией.

## Runtime-путь и snapshots

```text
source_system + event_type
        ↓
PostgresRouteConfigurationProvider
        ↓
включённые строки IntegrationRoute
        ↓
BUILTIN transformation, выбранный по transformation_key
        ↓
RouteProcessingResult + Processing Outbox в одной транзакции
        ↓
Kafka delivery.requested → Delivery Inbox → неизменяемая Delivery
```

Временный registry `BUILTIN` сопоставляет ключ из конфигурации с существующим Python-кодом преобразования. Он не выбирает target или route. В M7 его можно заменить на `FIELD_MAPPING_V1`, не меняя lookup маршрутов и Kafka-contract.

`delivery.requested` сохраняет URL, HTTP method, timeout получателя, преобразованный payload, identity route и retry policy. Позднее изменение конфигурации влияет только на новые Messages; существующая Delivery и её retry используют исходный snapshot.

Отключённый route не совпадает с новыми событиями. Отключённая source system создаёт configuration error для всего Message. Отключённые target system, policy, transformation или schema создают `CONFIGURATION_ERROR` для конкретного route без Delivery; остальные fan-out routes остаются независимыми.

## Проверки и границы M6

`tests/test_configuration_context.py` проверяет идемпотентный seed, ограничения БД, data-driven fan-out, callback routes, mutation маршрутов, отключённые системы, snapshots URL/retry и reprocessing исторического `warehouse-1c` callback. `tests/test_warehouse_to_mini_shop.py` отдельно защищает нормализованный callback source `1c-warehouse`. Полный backend-набор: `71 passed`, включая M5 Outbox/Inbox regression tests.

M6 сознательно не реализует `FIELD_MAPPING_V1`, validation/preview, lifecycle DRAFT/ACTIVE/RETIRED, Secret Provider, cache, Admin UI, Operational Query API и manual requeue UI. Это задачи следующих milestones.

## Живые проверки Docker / 1С

Configuration Context также проверен на локальном Docker-окружении и опубликованном HTTP-сервисе 1С.

- Успех: заказ Mini Shop `176e7671-c642-4506-85c5-249eaae42fa8` нашёл CRM и Warehouse routes из БД; обе Delivery получили статус `DELIVERED` с первой попытки. Callback 1С `2d325362-e541-4eba-84ca-700b8b3ae08b` нашёл success-route, после чего заказ Mini Shop стал `CONFIRMED`.
- Бизнесовый отказ: заказ `SKU-002 × 10` с ID `a02ca3df-352e-42c1-9ba7-73626ddd975e` был принят Warehouse. Callback `reservation.failed` с ID `e785123e-ca18-4833-8e77-fd5ffb597be8` прошёл через failure-route и перевёл заказ Mini Shop в `REJECTED`.
- Runtime mutation: пока `mini-shop-order-created-to-crm` имел `enabled = false`, заказ `14add648-ac90-4080-a62e-7b999829b8cb` создал только Warehouse Delivery; CRM Delivery не создавалась. Сразу после проверки route был возвращён в `enabled = true`. Warehouse callback всё равно дошёл до Mini Shop и завершил этот заказ статусом `REJECTED`.

Первый живой вызов Warehouse временно вернул HTTP 500. Его Delivery с действовавшими тогда baseline seed настройками — три попытки с паузами 5/10 секунд — стала `DEAD_LETTER`, сохранив request snapshot. После восстановления 1С новый обычный заказ прошёл успешно. Затем текущее локальное runtime-состояние policy было изменено через `configuration.retry_policies` для новых Delivery: пять попыток с паузами 15/30/60/120 секунд. Baseline seed при этом не менялся и остаётся равным 3 попыткам с паузами 5/10 секунд; повторный seed не перезаписывает локальную operator mutation. Для изменения running policy не требовались код или пересборка image.
