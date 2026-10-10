# Milestone 9 — Operational Query API и безопасный Manual Requeue

## Цель

M9 добавляет операторский read-side для расследования интеграций и безопасную
recovery-команду для `DEAD_LETTER` Delivery. API только показывает уже
сохранённые факты: он не является source of truth и не заменяет Kafka UI.

## Operational Query API

Публичные для будущего admin UI read endpoints:

- `GET /api/v1/admin/messages` — список сообщений, фильтры и opaque cursor;
- `GET /api/v1/admin/messages/{id}` — canonical payload, исторические RPR и
  все Delivery;
- `GET /api/v1/admin/messages/{id}/timeline` — упорядоченная timeline;
- `GET /api/v1/admin/correlations/{correlation_id}` — trace через Message,
  Delivery и попытки;
- `GET /api/v1/admin/deliveries`, `GET /api/v1/admin/deliveries/{id}`;
- `GET /api/v1/admin/dead-letters`.

Message list поддерживает фильтры source system, event type, domain status,
derived outcome, correlation id, external event id, message id и временной
интервал. Delivery list поддерживает target system, route, status, error
category, correlation/message id и временной интервал.

`outcome` вычисляется из сохранённых Message/RPR/Delivery фактов и не
записывается обратно в доменную модель. При таком фильтре API сканирует
батчами до следующего matching record, поэтому cursor не теряет сообщение,
между которым и предыдущим результатом находятся записи с другим outcome.

Для старых Warehouse callbacks API возвращает честное состояние:

```json
{
  "canonical_payload": {"external_order_id": "..."},
  "raw_payload": null,
  "raw_payload_available": false
}
```

Raw XML не сохранён M8, поэтому он не реконструируется и не выдаётся за
оригинальное входящее сообщение.

## Delivery lineage и requeue

Одна `RouteProcessingResult` имеет ровно одну исходную Delivery, но может
иметь цепочку recovery Delivery:

```text
initial Delivery (DEAD_LETTER)
    └─ requeue #1 (QUEUED → terminal)
         └─ requeue #2 (QUEUED → terminal)
```

Миграция `20261010_0011` заменяет глобальную уникальность
`route_processing_result_id` на partial unique index только для исходной
Delivery (`requeued_from_delivery_id IS NULL`). Дополнительно одна Delivery
может иметь только одного прямого child: повтор одной operator-команды
возвращает уже созданную recovery Delivery, а не создаёт второй запрос.

`POST /api/v1/admin/dead-letters/{delivery_id}/requeue`:

1. принимает только `DEAD_LETTER` (`404` для отсутствующей и `409` для
   неподходящей Delivery);
2. в одной транзакции создаёт новую `QUEUED` Delivery;
3. наследует message/RPR/correlation, payload, target, timeout и retry
   snapshots, а также исходный `idempotency_key`;
4. сохраняет `requeued_from_delivery_id`;
5. не публикует `delivery.requested` и не запускает HTTP синхронно.

Дальше уже существующий polling worker забирает `QUEUED` запись и выполняет
обычный transport/retry flow.

### Warehouse business-idempotency

Recovery для Warehouse безопасна не только transport-level idempotency key.
Опубликованный handler 1С обеспечивает business-idempotency по
`OrderNumber + RequestFingerprint`:

```text
нет ReservationRequest с таким OrderNumber
  → создать заявку, 202 RECEIVED

есть заявка и fingerprint совпадает
  → не создавать второй документ / резерв
  → вернуть её текущий state (2xx)

есть заявка, fingerprint отличается
  → 409 IDEMPOTENCY_CONFLICT
```

Это живо подтверждено Warehouse тестом order `700`: исходный request
`700|SKU-001:20` был создан один раз; другой payload с тем же order number
получил `409`; идентичный повтор после terminal `FAILED` вернул `200 FAILED`
без нового ReservationRequest. Поэтому даже если 1С сохранила заявку, но её
HTTP response был потерян, M9 requeue отправляет тот же XML snapshot и не
может создать второй business reservation.

Warehouse `2xx` означает только принятие/дедупликацию команды. Финальное
`SUCCEEDED` или `FAILED` остаётся асинхронным 1С business outcome и приходит
в Platform новым callback event с самостоятельным EventId.

## Граница с Message Completion

Recovery не переписывает исторический результат Processing Context:

```text
initial Delivery terminal
  → delivery.completed / delivery.dead_lettered
  → MessageCompletionConsumer

recovery Delivery terminal
  → delivery.recovery.completed
  → delivery.recovery.dead_lettered (если recovery снова dead-letter)
  → только operational read-side / Delivery lineage
```

Integration API слушает только первые две normal topics. Поэтому recovery
terminal event не может повторно создать Processing Inbox запись, конфликтовать
с terminal projection того же RPR или задним числом изменить `Message`.
В derived outcome Message учитываются только исходные Delivery; recovery facts
видны отдельно как `recovery_delivery_count` и в `lineage`.

## Проверка реализации

- Unit/integration tests покрывают отсутствие Delivery, запрет requeue для
  non-`DEAD_LETTER`, повтор той же команды, цепочку `D1 → D2 → D3`,
  наследование idempotency key и отсутствие normal completion event у recovery.
- Operational API tests покрывают summary/details/timeline/correlation,
  canonical payload without raw XML, historical configuration metadata,
  derived outcomes и cursor с non-matching сообщениями между страницами.
- Live PostgreSQL: контейнер Integration API применил migration head
  `20261010_0011`; `/api/v1/admin/messages` и message/correlation details
  вернули реальные M8 события.
- Live recovery: старый безопасный Mini Shop `DEAD_LETTER`
  `8128e124-a9dd-4de7-8da5-a5ba51a4839b` создал recovery
  `335c1a4e-3f29-439c-9030-13b6e82dbf86`. Она сохранила исходный idempotency
  key и parent id, выполнила retry policy и снова завершилась `DEAD_LETTER`.
  Delivery Outbox содержит только `delivery.recovery.completed` и
  `delivery.recovery.dead_lettered`; историческое Message осталось
  `ROUTING_FAILED` / `FAILED`. Повтор POST вернул ту же recovery Delivery с
  `created=false`.
- Live successful recovery: для реального ожидающего Mini Shop order
  `e7757656-f376-4dc8-8a3b-6ffebcb3eb85` был принят валидный Warehouse
  `reservation.succeeded` callback. При временно выключенном Mini Shop исходная
  Delivery `54cd4c0c-8c81-442e-8974-939fd08f69f2` исчерпала все пять попыток
  (`15/30/60/120` seconds) и стала `DEAD_LETTER` с `CONNECTION_ERROR`. После
  запуска Mini Shop M9 requeue создал `8d00e1fc-909c-406e-970d-cc7bd274bd0f`.
  Она сохранила тот же idempotency key, завершилась `DELIVERED` с первой
  попытки, а order перешёл из `WAITING_FOR_RESERVATION` в `CONFIRMED`.

## Ограничения

- Это query API, не полноценный operator UI; UI планируется следующим слоем.
- Auth profiles не snapshot-ятся на Delivery, а секреты никогда не возвращаются
  API.
- Requeue не отменяет и не редактирует исходную Delivery. Для исправления
  business payload или route configuration нужен новый бизнесовый event либо
  новая configuration version, а не recovery одной и той же snapshot.
