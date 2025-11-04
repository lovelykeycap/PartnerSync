# PartnerSync

Сервис на C# для сверки заказов внешних B2B-партнёров с внутренней системой. Периодически опрашивает REST API, сравнивает статус, сумму, валюту и версию заказа. Если данные расходятся, создаёт команду исправления и применяет её через gRPC.

Подходит как основа интеграции с поставщиками и маркетплейсами, где партнёр отвечает за перечисленные поля заказа. Репозиторий включает демо-API партнёра и внутреннюю gRPC-систему.

## Что реализовано

- Опрос нескольких партнёров, cursor pagination, таймауты, повторы HTTP GET при сетевых ошибках, `429` и `5xx`.
- Общий rate limit на партнёра через Redis и Lua. PostgreSQL advisory lock исключает одновременный обход одного партнёра несколькими экземплярами.
- Transactional outbox: результат сверки и команда исправления сохраняются в одной транзакции PostgreSQL.
- Kafka producer с подтверждением брокера, consumer с ручным commit offset, ограниченные повторы gRPC и Dead Letter Queue.
- Transactional inbox на принимающей стороне, дедупликация по ID события, защита от устаревших версий и обновление по ожидаемой ревизии.
- API диагностики и replay, health endpoints, JSON-логи, счётчики `System.Diagnostics.Metrics`.

Стек: .NET 10, ASP.NET Core, Npgsql, PostgreSQL 17, Kafka 4.1 в KRaft-режиме, gRPC и Protobuf, Redis 7.4, Docker Compose. Сборка и запуск при создании проекта не выполнялись.

## Запустить демосценарий

Нужны Docker Engine или Docker Desktop с Compose v2. Устанавливать .NET на хост не требуется.

```bash
docker compose up --build -d
```

Проверить готовность сервиса:

```bash
curl -f http://localhost:8080/health/ready
```

При старте демобаза содержит три заказа партнёра. Внутри системы `A-1001` имеет устаревший статус, `A-1002` отсутствует, `A-1003` уже совпадает. Ожидаемый результат после обработки событий и следующего прохода — три записи `consistent`:

```bash
curl -H 'X-Api-Key: local-admin-key' \
  http://localhost:8080/api/reconciliation

curl -H 'X-Api-Key: local-partner-key' \
  http://localhost:8081/demo/internal/orders
```

Интервал между проходами — 30 секунд после завершения предыдущего. Изменить заказ у партнёра:

```bash
curl -X PUT http://localhost:8081/demo/orders/A-1001 \
  -H 'X-Api-Key: local-partner-key' \
  -H 'Content-Type: application/json' \
  -d '{"status":"shipped","totalMinor":149900,"currency":"RUB"}'
```

API увеличит версию источника. Следующий проход создаст команду, а consumer обновит внутренний заказ. `149900` — сумма в копейках, то есть 1499 рублей.

Посмотреть логи и состояние outbox:

```bash
docker compose logs -f partner-sync
```

```bash
curl -H 'X-Api-Key: local-admin-key' http://localhost:8080/api/outbox
```

Остановить контейнеры с сохранением данных:

```bash
docker compose down
```

## Подключить партнёра

Добавьте элемент в `Sync:Partners` в [appsettings.json](src/PartnerSync/appsettings.json). Укажите уникальный `Id`, `BaseUrl` с завершающим `/`, `ApiKey` и `RequestsPerSecond`. Настройки переопределяются переменными окружения, например `Sync__Partners__0__ApiKey`.

Адаптер ожидает `GET {BaseUrl}orders?limit=100&cursor=...`, ключ в `X-Api-Key` и ответ:

```json
{
  "items": [
    {
      "orderId": "A-1001",
      "status": "paid",
      "totalMinor": 149900,
      "currency": "RUB",
      "version": 2
    }
  ],
  "nextCursor": null
}
```

Допустимые статусы: `created`, `paid`, `shipped`, `cancelled`. `version` — положительное целое, которое растёт при любом изменении заказа. Валюта передаётся тремя заглавными латинскими буквами. `nextCursor: null` завершает обход. Страницы не должны содержать больше 100 заказов. Для другого формата API измените [PartnerClient](src/PartnerSync/Infrastructure/PartnerClient.cs).

Внутренняя система реализует [orders.proto](src/PartnerSync.Contracts/Protos/orders.proto). Адрес задаётся в `Sync:InternalGrpcUrl`, ключ — в `Sync:InternalApiKey`. Изменение заказа и запись inbox должны оставаться одной транзакцией. Демореализация находится в [OrdersService](src/DemoSystems/OrdersService.cs).

## Навигация

| Путь | Содержимое |
| --- | --- |
| `src/PartnerSync` | API, опрос партнёров, сверка, outbox publisher, Kafka consumer |
| `src/PartnerSync.Contracts` | Protobuf-контракт и сообщения Kafka |
| `src/DemoSystems` | REST API партнёра и внутренняя gRPC-система |
| `deploy/postgres/001_initial.sql` | Схема БД и начальные данные |
| [Архитектура](docs/architecture.md) | Правила сверки, гарантии доставки, границы реализации |
| [Эксплуатация](docs/operations.md) | Эндпоинты, настройки, DLQ replay и локальная разработка |
