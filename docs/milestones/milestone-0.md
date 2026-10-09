# Milestone 0 — стартовая инфраструктура

**Статус:** завершён 2026-09-20.

Этот документ восстановлен по исходным commits M0 во всех репозиториях. В M0
создавалась воспроизводимая локальная среда и минимальные запускаемые каркасы;
бизнес-интеграция ещё не реализовывалась.

## Цель

Подготовить изолированные репозитории и Docker Compose-окружение, в котором
будущие сервисы смогут стартовать, применять собственные миграции и проверять
доступность своих баз данных.

```text
Docker Compose
  ├─ PostgreSQL
  │    ├─ integration_db
  │    ├─ shop_db
  │    └─ crm_db
  ├─ Kafka
  ├─ Integration API
  ├─ Delivery Service
  ├─ Mini Shop
  ├─ CRM
  └─ Admin UI placeholder

Warehouse 1С — вне Docker Compose
```

## Исходные commits

| Репозиторий | Commit M0 | Что было подготовлено |
| --- | --- | --- |
| `integration-platform` | `8befa2a` | Integration API, Delivery Service, общие DB/health utilities, Alembic и Admin UI placeholder. |
| `integration-environment` | `1ae6565` | Docker Compose, PostgreSQL init, `.env.example`, Makefile и scripts запуска. |
| `demo-business-systems` | `57ee6e4` | Независимые каркасы Mini Shop и CRM с Docker, Alembic и health checks. |
| `warehouse-1c` | `060a77f` | Каркас репозитория для будущих артефактов конфигурации 1С. |

## Среда запуска

Compose создавал один локальный контур со следующими публичными адресами:

| Компонент | Адрес с хоста |
| --- | --- |
| Integration API | `http://localhost:8000` |
| Delivery Service | `http://localhost:8001` |
| Mini Shop | `http://localhost:8002` |
| CRM | `http://localhost:8003` |
| Admin UI | `http://localhost:5173` |
| PostgreSQL | `localhost:5433` |
| Kafka | `localhost:9092` |

PostgreSQL init-script создавал отдельные базы `integration_db`, `shop_db` и
`crm_db` с раздельными учётными данными. Сервисам не разрешалось выполнять
кросс-базовые бизнес-запросы: они должны были интегрироваться только через
внешние контракты.

Для запуска были добавлены `make up`, `make build`, `make logs`, `make down`,
`make reset` и `make test`, а также эквивалентные shell scripts. `make reset`
является намеренно разрушительной командой: удаляет Docker volumes и нужен
только для чистого локального старта.

## Integration Platform

В M0 появились:

- Dockerfiles для Integration API и Delivery Service;
- общие настройки подключения к БД;
- Alembic-конфигурация и первая миграция
  `20260919_0001_platform_service_state`;
- `GET /health/live` и `GET /health/ready` у обоих процессов;
- проверка готовности через `SELECT 1` к `DATABASE_URL`;
- минимальный Vite Admin UI без бизнес-функций;
- тесты health checks и миграций.

## Mini Shop и CRM

Для каждой демонстрационной системы подготовлены отдельно:

- Python-проект, Dockerfile, настройки и Alembic;
- собственная первая миграция service state;
- `GET /health/live` и `GET /health/ready`;
- тесты health checks и миграций.

Несмотря на общий репозиторий `demo-business-systems`, Mini Shop и CRM с самого
начала имеют независимые приложения, базы данных, миграции, Docker images и
внешние API.

## Warehouse 1С

В M0 Warehouse намеренно не заменялся Python-сервисом и не добавлялся в
Compose. Был подготовлен только каркас:

```text
warehouse-1c/
├─ configuration/
├─ extensions/
├─ http-services/
└─ docs/
```

Реальная конфигурация, информационная база, HTTP-публикация Apache и сценарии
резервирования появились за пределами M0.

## Явно вне рамок M0

- бизнес-сущности заказов, клиентов, сделок и товарных позиций;
- `POST /api/orders` Mini Shop;
- приём `order.created` в Integration Platform;
- `Message`, routing, transformation и HTTP-доставка в CRM;
- outbox, retry, DLQ и delivery worker;
- Kafka consumers/producers;
- HTTP endpoint и исходящие события Warehouse 1С;
- реальная функциональность Admin UI.

Kafka присутствовал только как инфраструктурный компонент, без runtime-flow.

## Итог

Milestone 0 создал стабильную техническую основу: сервисы запускались через
один Compose, имели собственные БД и migrations, а readiness проверяла реальное
соединение с PostgreSQL. Бизнесовый сквозной сценарий был сознательно отложен
на Milestone 1.
