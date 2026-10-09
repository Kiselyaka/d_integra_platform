# Milestone 2 — fan-out `order.created` в CRM и Warehouse 1С

**Статус:** завершён и проверен на запущенных локальных сервисах.

## Цель

Один внешний event Mini Shop должен быть принят Platform один раз и независимо
доставлен двум получателям:

```text
Mini Shop
  └─ POST /api/v1/events: mini-shop / order.created / v1
       └─ Integration Platform: один Message
            ├─ CRM: JSON-запрос на создание сделки
            └─ Warehouse 1С: XML ReservationRequest
                 └─ Apache :8081 → опубликованная информационная база 1С
```

Warehouse 1С асинхронно обрабатывает заявку. Поздний результат
`ReservationSucceeded` или `ReservationFailed` возвращается в Platform по
контракту Milestone 1+; его маршрут к Mini Shop не входит в M2.

## Граница ответственности

- Platform запускается в Docker на `:8000`.
- 1С Предприятие и информационная база Warehouse работают на Windows-хосте.
- Apache публикует HTTP-сервис 1С на `:8081`.
- Поэтому контейнер Platform обращается к 1С не через `localhost`, а через
  `host.docker.internal`:

  ```text
  http://host.docker.internal:8081/WarehouseDemo_Base/hs/reservations/
  ```

  Значение задаётся настройкой `WAREHOUSE_BASE_URL` без завершающего пути
  `/hs/reservations/`.

Репозиторий `warehouse-1c` не содержит живую информационную базу. Настройка
публикации Apache и код конфигурации исполняются в отдельной рабочей базе 1С;
для M2 она использовалась как реальный внешний сервис.

## Маршрутизация и результаты

Для source route
`mini-shop / order.created / schema_version = 1` теперь совпадают **два**
маршрута:

| Route key | Получатель | Формат | Успех транспорта |
| --- | --- | --- | --- |
| `mini-shop-order-created-to-crm` | CRM | JSON | успешный HTTP-ответ CRM |
| `mini-shop-order-created-to-warehouse` | Warehouse 1С | XML | только `202 Accepted` |

Один входящий event создаёт одну запись `Message` и две записи
`RouteProcessingResult`. Уникальность результата обеспечена ограничением БД:
`(message_id, route_key, route_version)`.

Результаты каждого маршрута изолированы:

- ошибка преобразования сохраняется как `TRANSFORMATION_FAILED` только для
  этого маршрута;
- ошибка транспорта сохраняется как `DELIVERY_FAILED` только для этого
  маршрута;
- обработка второго маршрута продолжается всегда;
- `Message = COMPLETED`, когда успешны оба маршрута;
- `Message = ROUTING_FAILED`, когда неуспешен хотя бы один;
- `Message = NO_ROUTE`, если совпавших маршрутов нет.

Успешный `RouteProcessingResult` имеет существующий статус
`DELIVERY_REQUESTED`. В пределах M2 он означает, что синхронный HTTP-запрос к
этому получателю успешно принят; отдельный агрегат delivery, повторная доставка
и worker не добавлялись.

## Контракт Platform → Warehouse 1С

Полная спецификация: [Warehouse ReservationRequest v1](../contracts/warehouse-reservation-request-v1.md).

Platform строит запрос из валидного payload Mini Shop:

```xml
<?xml version="1.0" encoding="utf-8"?>
<ReservationRequest>
    <OrderNumber>fbfee07b-9a9d-4b0e-a814-6c50d4c63371</OrderNumber>
    <Positions>
        <Position>
            <ItemCode>SKU-001</ItemCode>
            <Quantity>1</Quantity>
        </Position>
    </Positions>
</ReservationRequest>
```

| Mini Shop `order.created` | XML Warehouse |
| --- | --- |
| `payload.order_id` | `ReservationRequest/OrderNumber` |
| `payload.items[*].product_code` | `Positions/Position/ItemCode` |
| `payload.items[*].quantity` | `Positions/Position/Quantity` |

Название товара, цена, клиент и адрес намеренно не передаются: их нет в
согласованном контракте резервирования.

`202 Accepted` подтверждает только создание `ReservationRequest` в 1С для
асинхронной обработки. Он не подтверждает наличие товара; окончательный
результат передаётся callback-событием 1С.

### Идентификаторы

Mini Shop генерирует `order_id` как UUID v4. Этот технический идентификатор
последовательно используется как:

```text
Mini Shop Order.id
  → order.created.payload.order_id
  → ReservationRequest.OrderNumber / External order id в 1С
  → ReservationSucceeded|Failed.OrderNumber
```

Также Mini Shop формирует `event_id` как `order-created-<order_id>`. UUID
нужен для однозначной связи систем и идемпотентности. Человеко-читаемый короткий
номер заказа в M2 не добавлялся; если он понадобится, его следует вводить
отдельным полем, не заменяя технический UUID.

## Реализация в Platform

- `RouteProvider` заменён на `find_matching_routes()`, возвращающий список
  маршрутов, а не один маршрут.
- `StaticRouteProvider` хранит CRM- и Warehouse-маршрут для `order.created`.
- Добавлены `TargetPayload` и `RouteTransformationPort`.
- `RouteTransformationProvider` выбирает JSON-преобразование для CRM либо
  XML-преобразование `OrderCreatedToWarehouseReservationTransformation` для
  Warehouse.
- XML создаётся стандартным `ElementTree`, поэтому специальные символы
  экранируются корректно.
- CRM отправляется через `CrmHttpTargetDeliveryAdapter` на
  `/api/integration/orders` как JSON.
- Warehouse отправляется отдельным `WarehouseHttpTargetDeliveryAdapter` на
  `/hs/reservations/` с `Content-Type: application/xml; charset=utf-8`; любой
  код кроме `202` является ошибкой доставки.
- `RouteTargetDeliveryAdapter` выбирает конкретный адаптер по
  `target_system` маршрута.
- Добавлены `warehouse_base_url` и
  `warehouse_delivery_timeout_seconds` в настройки Platform; Compose передаёт
  `WAREHOUSE_BASE_URL`.
- Миграция `20261009_0003_route_result_per_route.py` добавляет ограничение
  уникальности результата маршрута.

Generic endpoint `POST /api/v1/events` и Warehouse inbound endpoint
`POST /api/v1/events/warehouse` остаются разными входными адаптерами. Внутри
они используют общий ingestion pipeline и одну защиту от дубликатов
`(source_system, external_event_id)`.

## Проверки

### Автоматические

Добавлены unit-тесты, не требующие доступа к 1С:

- подбор ровно двух маршрутов для `order.created`;
- отсутствие маршрута для Warehouse callback в M2;
- все четыре сочетания исходов CRM/Warehouse;
- преобразование нескольких позиций в XML и экранирование XML;
- URI, метод, заголовок, timeout и строгий `202` для Warehouse adapter;
- ошибки не-`202` и ошибки соединения;
- отправка CRM именно JSON-body, а не внутреннего объекта `TargetPayload`.

Выполненная выборка:

```text
python -m pytest tests/test_route_matching.py tests/test_fanout_processing.py tests/test_warehouse_delivery.py -q
12 passed
```

Также успешно выполнены `python -m compileall -q src` и создание приложения
`create_app()`.

Полный набор backend-тестов на Windows в текущем окружении остаётся ограничен
старой проблемой `FastAPI TestClient`/AnyIO: он зависает ещё на исходном
`test_event_ingestion.py` при создании socketpair. Это состояние было
зафиксировано до M2 и не связано с fan-out. Реальные HTTP-проверки ниже
выполнены в Docker.

### Реальный E2E

1. Первый запуск с заказом `686dfe94-4881-41b5-bcd7-191893ee4a26` подтвердил
   независимость маршрутов: CRM обнаружил ошибку сериализации, а Warehouse
   получил XML, вернул `202`, создал заявку и позже отправил
   `ReservationSucceeded`. Ошибка CRM была исправлена.
2. Повторный сквозной запуск с заказом
   `fbfee07b-9a9d-4b0e-a814-6c50d4c63371` завершился успешно:
   - один `Message` — `29fea8ee-edac-488c-b4d3-4a6112fc2b61` — имеет статус
     `COMPLETED`;
   - существуют два `RouteProcessingResult` со статусом
     `DELIVERY_REQUESTED`;
   - в CRM создана одна сделка с этим `external_order_id`;
   - Warehouse 1С принял XML с `202` и создал `ReservationRequest №15` с
     позицией `SKU-001`, количеством `1`;
   - 1С обработала заявку и сформировала `reservation.succeeded` с отдельным
     `EventId`; событие имеет статус «Отправлено» и `Retry count = 0`.
3. Идемпотентность проверена повторной отправкой **идентичного** `order.created`
   с тем же `event_id = order-created-fbfee07b-9a9d-4b0e-a814-6c50d4c63371`.
   Platform ответила:

   ```json
   {
     "message_id": "29fea8ee-edac-488c-b4d3-4a6112fc2b61",
     "status": "already_accepted"
   }
   ```

   После повтора в Platform осталась одна запись `Message` и два прежних
   результата маршрутов; в CRM — одна сделка; в 1С не появилось ни нового
   `ReservationRequest`, ни нового `OutboundEvent`.

## Вне рамок M2

- доставка Warehouse callback в Mini Shop;
- повторные доставки и retry worker Platform;
- outbox/inbox Platform, Kafka и delivery aggregate;
- конфигурация маршрутов в БД или UI;
- короткий отображаемый номер заказа;
- изменение кода и метаданных рабочей конфигурации 1С.

Следующий логический этап — Milestone 3: направить принятый
`warehouse.reservation.succeeded` / `warehouse.reservation.failed` из Platform
в Mini Shop.
