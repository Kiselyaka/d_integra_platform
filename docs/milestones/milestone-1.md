# Milestone 1 — итоговый отчёт реализации

> Статус на 2026-10-04: завершён. Реализованы M1.1–M1.6; локальные тесты, Docker migrations, health checks и воспроизводимый E2E-сценарий прошли успешно.

## 1. Цель milestone

Реализуется один узкий сквозной сценарий:

```text
Mini Shop
  POST /api/orders
      ↓
shop_db: orders + order_items
      ↓ HTTP external event: order.created
Integration API
  POST /api/v1/events
  messages + route_processing_results
      ↓ route + transformation + direct HTTP request
CRM
  POST /api/integration/orders
      ↓
crm_db: customers + deals + deal_positions
```

Этот milestone намеренно не является полной платформой интеграции. В нём один статический маршрут (`mini-shop/order.created/v1 → CRM`) и синхронная доставка.

## 2. Затронутые репозитории

| Репозиторий | Назначение изменений M1 |
| --- | --- |
| `integration-platform` | Публичный event contract, приём, валидация, Message runtime model, route abstraction, transformation, CRM HTTP adapter. |
| `demo-business-systems/mini-shop` | Реальная модель заказа, создание заказа и HTTP-publisher внешнего события. |
| `demo-business-systems/crm` | Локальная CRM-модель и endpoint для приёма преобразованного заказа. |
| `integration-environment` | Конфигурация `CRM_BASE_URL` и корректное внутреннее сетевое сопоставление CRM. |
| `warehouse-1c` | Не изменялся: 1С и JSON→XML вне scope M1. |

Python-код между Mini Shop, CRM и Platform не импортируется. Взаимодействие происходит только по опубликованным JSON/HTTP-контрактам.

## 3. M1.1 — опубликованный contract

Integration API принимает:

```http
POST /api/v1/events
Content-Type: application/json
```

Generic envelope v1 содержит:

| Поле | Назначение |
| --- | --- |
| `event_id` | Внешний идентификатор события. Используется для inbound idempotency. |
| `event_type` | Тип события; в M1 только `order.created`. |
| `source_system` | Код системы-источника; в M1 только `mini-shop`. |
| `occurred_at` | ISO-8601 время возникновения события. |
| `correlation_id` | Необязателен в generic contract; Mini Shop передаёт `order-<order_id>`. |
| `schema_version` | Положительная версия схемы; в M1 равна `1`. |
| `payload` | JSON object с source-specific данными. |

Реализация и документация:

- `integration-platform/backend/src/integration_api/message_processing/contracts.py`
- `integration-platform/docs/contracts/external-event-envelope-v1.md`
- `integration-platform/docs/contracts/mini-shop-order-created-v1.md`

Source payload `mini-shop/order.created/v1` содержит идентификатор заказа, customer, shipping address, позиции, валюту и сумму. Денежные значения передаются decimal-compatible строками (`"10.50"`, `"21.00"`). В Mini Shop внутренний `status` заказа намеренно не публикуется.

## 4. M1.2 — Mini Shop

### Domain tables

Новая Alembic revision `20261004_0002_shop_domain` создаёт в `shop_db`:

| Таблица | Содержимое |
| --- | --- |
| `products` | Каталог: `id`, стабильный `product_code`, имя, цена. |
| `customers` | Клиент Mini Shop с business `customer_id`. |
| `orders` | Заказ, customer, валюта, итоговая сумма, статус `CREATED`, адрес доставки, дата создания. |
| `order_items` | Снимок позиции заказа: product code, название, quantity и unit price. |

В migration добавлен воспроизводимый demo product:

```text
SKU-001 | Example Product | 10.50 EUR
```

### API и прикладная логика

Добавлен endpoint:

```http
POST /api/orders
```

Он:

1. находит `Product` по `product_code`;
2. берёт цену только из `shop_db`;
3. рассчитывает `total_amount`;
4. сохраняет `Customer`, `Order` и `OrderItem`;
5. формирует `order.created` envelope;
6. отправляет его в Platform через отдельный порт `IntegrationEventPublisher` и adapter `HttpIntegrationEventPublisher`.

Код разделён на `domain`, `application` и `infrastructure`. HTTP-клиент не находится в domain entity.

### Текущее ограничение Mini Shop

Сначала фиксируется заказ, потом производится одна синхронная попытка публикации. При недоступной Platform API возвращает понятную ошибку, событие логируется как неотправленное, но Outbox не создаётся. Это сознательный временный shortcut M1; надёжная публикация будет отдельной будущей задачей.

## 5. M1.3 — приём событий Platform

### Runtime tables

Alembic revision `20261004_0002_message_runtime` создаёт в `integration_db`:

| Таблица | Назначение |
| --- | --- |
| `messages` | Принятый external event: source/external ID, тип, payload, status, correlation, version, timestamps, validation error. |
| `route_processing_results` | Исполнение маршрута: message, route key/version, status, error и timestamps. |

На `messages` действует database constraint:

```text
UNIQUE(source_system, external_event_id)
```

Это фундамент inbound idempotency: повторная доставка одного события не создаёт второй `Message`.

### Поведение endpoint

`POST /api/v1/events` выполняет Pydantic validation generic envelope, затем:

- для нового события сохраняет `Message` и возвращает `202 {message_id, status: "accepted"}`;
- для duplicate возвращает существующий `message_id` и `202 {status: "already_accepted"}`;
- для невалидного source payload создаёт `Message` со статусом `VALIDATION_FAILED`, не падая с 500.

Source validation вынесена за порт `SourceValidationPort`, а текущий adapter `PydanticSourceValidator` понимает только `mini-shop/order.created/1`. В будущем этот adapter можно заменить versioned JSON Schema subsystem без переписывания HTTP controller.

## 6. M1.4 — CRM

### Domain tables

Alembic revision `20261004_0002_crm_domain` создаёт в `crm_db`:

| Таблица | Важные поля и ограничения |
| --- | --- |
| `customers` | `external_customer_id UNIQUE`, имя, email, phone, timestamps. |
| `deals` | `external_order_id UNIQUE`, customer FK, amount, currency, timestamps. |
| `deal_positions` | Deal FK, product code, title, quantity, price; unique `(deal_id, external_product_code)`. |

### Integration endpoint

CRM принимает только свой target contract:

```http
POST /api/integration/orders
```

Endpoint создаёт или обновляет Customer по `external_customer_id`, создаёт или обновляет Deal по `external_order_id`, затем сохраняет позиции. Повторный request одного внешнего заказа не создаёт новый Deal: обновляются разрешённые поля и состав позиций.

CRM не получает raw source payload Mini Shop. Это обеспечивает независимость его внутреннего контракта от source system.

## 7. M1.5 — Platform route, transformation и доставка

### Route abstraction

Введён `RouteProvider`. Текущий `StaticCrmRouteProvider` содержит единственный M1 route:

```text
mini-shop + order.created + schema_version 1
  → mini-shop-order-created-to-crm (route version 1)
```

Маршрут не определён в FastAPI handler. В будущем static adapter заменяется persistence/configuration adapter без изменения application pipeline.

### Transformation

`OrderCreatedToCrmTransformation` — отдельный application component. Он преобразует source payload в CRM target payload:

| Source | CRM target |
| --- | --- |
| `order_id` | `external_order_id` |
| `customer.customer_id` | `client.external_customer_id` |
| `customer.full_name/email/phone` | `client.name/email/phone` |
| `total_amount`, `currency` | `deal.amount`, `deal.currency` |
| `shipping_address` | `delivery_address` |
| `items.product_code/name/quantity/unit_price` | `positions.external_product_code/title/quantity/price` |

### Delivery adapter

`TargetDeliveryPort` скрывает транспорт от application layer. Реализация `HttpTargetDeliveryAdapter` вызывает:

```http
POST {CRM_BASE_URL}/api/integration/orders
Idempotency-Key: <external event_id>
```

Platform контейнер использует `CRM_BASE_URL=http://crm:8000`. CRM теперь слушает **внутри Docker network** порт `8000`; с хоста сохраняется публичный адрес `http://localhost:8003` через mapping `8003:8000`.

### Runtime states

| Ситуация | Message status | Route processing status |
| --- | --- | --- |
| Source payload invalid | `VALIDATION_FAILED` | запись route не создаётся |
| Нет подходящего route | `NO_ROUTE` | запись route не создаётся |
| Transformation error | `ROUTING_FAILED` | `TRANSFORMATION_FAILED` |
| CRM HTTP error | `ROUTING_FAILED` | `DELIVERY_REQUESTED` + structured `error` |
| CRM 2xx | `COMPLETED` | `DELIVERY_REQUESTED`, `completed_at` заполнено |

Последняя строка — временная семантика M1: `DELIVERY_REQUESTED` документированно означает, что единственный синхронный HTTP request получил 2xx. Отдельный `Delivery` aggregate, DeliveryAttempt и retry state machine здесь намеренно отсутствуют.

## 8. Логирование

Добавлены/используются следующие structured event names без полного payload и без secrets:

```text
order_created
external_event_sent
message_received
duplicate_event_detected
route_matched
transformation_completed
crm_delivery_started
crm_delivery_completed
crm_delivery_failed
```

## 9. Тесты, выполненные до текущего момента

| Система | Команда | Результат |
| --- | --- | --- |
| Integration Platform | `python -m pytest -q` в `integration-platform/backend` | `18 passed` (есть один не-блокирующий upstream warning Starlette TestClient). |
| CRM | `python -m pytest -q` в `demo-business-systems/crm` | `6 passed` (тот же warning). |
| Mini Shop | `python -m pytest -q` в `demo-business-systems/mini-shop` | `5 passed` на момент M1.2. |

Покрыты: generic contract, health, migration на чистой in-memory SQLite БД, создание Mini Shop order, вычисление суммы, publish success/failure, ingestion, duplicate Message, source validation error, no route, transformation, CRM delivery success/failure, CRM customer/deal upsert и contract validation.

Docker deployment проверен для migrations:

```text
integration_db → 20261004_0002 (messages, route_processing_results)
crm_db         → 20261004_0002 (customers, deals, deal_positions)
```

## 10. M1.6 — E2E и duplicate-event

Во время проверки были выявлены и исправлены две инфраструктурные проблемы:

1. Первый заказ был создан в момент, когда Integration API container ещё содержал M1.3 image. Он корректно попал в `messages`, но остался `VALIDATED`, потому что в той сборке отсутствовал M1.5 processing pipeline.
2. Второй заказ дошёл до route, но CRM delivery получил `Connection refused`: Platform использовал заданный M1 internal URL `crm:8000`, а CRM по наследству M0 слушал 8003 внутри контейнера. Dockerfile и Compose mapping исправлены на internal `8000` / external `8003`.

После исправления M1.5 код внутри запущенного Platform container был отдельно подтверждён. Финальная ручная E2E-проверка создала заказ `89126f09-2df0-4748-91d7-95bf57df5284`:

```text
shop_db         CREATED | 21.00 | 1 OrderItem
integration_db  Message COMPLETED | route mini-shop-order-created-to-crm | DELIVERY_REQUESTED | без ошибки
crm_db          C-M1-E2E-003 | тот же external_order_id | 21.00 EUR | 1 DealPosition
```

Тот же `order.created` envelope был повторно передан в Platform. Ответ: `202` с тем же `message_id` и `status: already_accepted`. Проверка БД после повтора:

```text
integration_db  1 Message | 1 RouteProcessingResult
crm_db          1 Customer | 1 Deal | 1 DealPosition
```

Для дальнейших повторных проверок добавлены:

```text
scripts/test-e2e.sh
make test-e2e
```

Скрипт создаёт уникальный заказ, проверяет все три БД, передаёт duplicate event и проверяет отсутствие дублей. Он был выполнен успешно:

```text
M1 E2E passed: order_id=ad887017-f2e1-4bbe-92ef-50940e0a5ed5
```

Финальные проверки окружения:

```text
Platform pytest  18 passed
Mini Shop pytest 5 passed
CRM pytest       6 passed

Integration API ready      200
Delivery Service ready     200
Mini Shop ready            200
CRM ready                  200
Admin UI                   200
PostgreSQL                 healthy
Kafka                      running (не используется runtime flow)
```

Новые migrations были проверены на чистой in-memory SQLite БД в тестах и применены к существующим Docker PostgreSQL БД через `alembic upgrade head`. Полный destructive reset среды намеренно не выполнялся, чтобы не удалять текущие демонстрационные данные.

## 11. Явные temporary shortcuts M1

- HTTP Platform → CRM выполняется синхронно в request path.
- Kafka есть в Compose, но не используется runtime flow.
- Нет Outbox у Mini Shop.
- Нет Delivery aggregate, DeliveryAttempt, retry scheduler, DLQ или manual requeue.
- Есть только один hard-coded/static route provider.
- Source validation реализована Pydantic adapter, не versioned JSON Schema subsystem.
- Transformation реализована одним явным `OrderCreatedToCrmTransformation`, не full FIELD_MAPPING_V1.
- Нет 1С, JSON→XML, Admin UI screens, AuthProfile, metrics/dashboards.

Все эти ограничения сознательны и соответствуют scope Milestone 1; они не должны быть скрыты или приниматься за production-grade delivery guarantees.

## 12. Итог Definition of Done

Все критерии M1 выполнены: Mini Shop сохраняет реальные заказы и публикует событие, Platform сохраняет Message, валидирует payload, определяет маршрут через abstraction, преобразует контракт и доставляет его в CRM через outbound port; CRM создаёт/обновляет Customer и Deal. Повторная отправка external event не создаёт дублей. Migrations, health checks, Compose и воспроизводимый E2E test проходят.

Следующий milestone автоматически не начинается.
