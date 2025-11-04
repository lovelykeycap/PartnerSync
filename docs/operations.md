# Запуск и диагностика PartnerSync

## Найти причину незавершённой сверки

Прочитайте результаты последнего прохода:

```bash
curl -H 'X-Api-Key: local-admin-key' \
  'http://localhost:8080/api/reconciliation?limit=100'
```

Для `pending` проверьте outbox и логи:

```bash
curl -H 'X-Api-Key: local-admin-key' http://localhost:8080/api/outbox
```

```bash
docker compose logs --tail=100 partner-sync
```

Если outbox опубликован, а заказ остаётся `pending` несколько проходов, прочитайте DLQ:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic partner-sync.repairs.dlq.v1 --from-beginning
```

Логи consumer содержат ID события и результат `Applied`, `Conflict` или `Stale`. `Conflict` означает, что внутренняя ревизия изменилась между чтением и записью. Сверка перечитает заказ. Состояние `conflict` в API означает более высокую версию источника во внутренней системе. Проверьте контракт версий и данные партнёра.

## Повторить событие из DLQ

Устраните причину сбоя. Сохраните одну JSON-строку из DLQ в `dead-letter.json`, затем отправьте исходное событие через replay API. Команда требует `jq`:

```bash
jq '{originalPayload}' dead-letter.json | \
  curl -X POST http://localhost:8080/api/repairs/replay \
    -H 'X-Api-Key: local-admin-key' \
    -H 'Content-Type: application/json' \
    --data-binary @-
```

API вернёт `requeued` после подтверждения Kafka. ID события сохраняется. Inbox предотвращает повторное применение уже обработанного запроса. Replay не удаляет запись из DLQ и не обходит проверки версии или ревизии. Событие с некорректным контрактом API отклонит.

## Воспроизвести расхождение без изменения партнёра

После завершения начальной синхронизации измените внутреннюю проекцию и увеличьте её ревизию:

```bash
docker compose exec postgres psql -U partnersync -d partnersync -c \
  "UPDATE internal_system.orders SET status = 'created', revision = revision + 1 WHERE partner_id = 'acme' AND order_id = 'A-1001';"
```

Следующий полный проход обнаружит расхождение и восстановит статус партнёра. Все внутренние операции записи должны увеличивать `revision`.

## Запустить C#-процессы вне Docker

Установите .NET SDK 10 и запустите зависимости:

```bash
docker compose up -d postgres redis kafka-init
```

В отдельных терминалах из корня репозитория запустите приложения:

```bash
dotnet run --project src/DemoSystems
```

```bash
ASPNETCORE_URLS=http://localhost:8080 dotnet run --project src/PartnerSync
```

Локальные адреса зависимостей уже заданы в `appsettings.json`. Полную сборку можно выполнить командой `dotnet build PartnerSync.slnx`. Результат этой команды в рамках создания репозитория не проверялся.

## Сбросить демоданные

Следующая команда удаляет PostgreSQL и Kafka volumes вместе со всеми заказами и сообщениями проекта:

```bash
docker compose down -v
```

Повторный `docker compose up --build -d` создаст начальные данные. Bootstrap SQL выполняется только на пустой базе.

## Справочник API

| Метод и путь | Доступ | Назначение |
| --- | --- | --- |
| `GET :8080/health/live` | Без ключа | Процесс обслуживает HTTP |
| `GET :8080/health/ready` | Без ключа | PostgreSQL, Redis, Kafka topics и gRPC отвечают; иначе 503 |
| `GET :8080/api/reconciliation?limit=50` | `AdminApiKey` | Последние результаты, максимум 200 |
| `GET :8080/api/outbox` | `AdminApiKey` | Число ожидающих и опубликованных событий, возраст очереди по `oldestPendingAt` |
| `POST :8080/api/repairs/replay` | `AdminApiKey` | Повторно отправить валидное событие; тело `{"originalPayload":"..."}` |
| `GET :8081/orders` | `PartnerApiKey` | Демо-выдача заказов партнёра |
| `PUT :8081/demo/orders/{id}` | `PartnerApiKey` | Изменить существующий заказ и увеличить версию |
| `GET :8081/demo/internal/orders` | `PartnerApiKey` | Просмотреть внутреннюю демопроекцию |
| gRPC на `:8082` | `InternalApiKey` | `GetOrder`, `ApplyPartnerOrder` |

Ключ передаётся заголовком `X-Api-Key`, для gRPC — metadata `x-api-key`. Демометоды управления данными находятся в отдельном приложении `DemoSystems`.

## Справочник конфигурации

Значения по умолчанию находятся в [appsettings.json](../src/PartnerSync/appsettings.json). Разделители `:` заменяются на `__` в переменных окружения.

| Настройка | Значение по умолчанию | Назначение |
| --- | --- | --- |
| `ConnectionStrings:Postgres` | Локальная БД `partnersync` | Хранилище сверки и outbox |
| `ConnectionStrings:Redis` | `localhost:6379,abortConnect=false` | Общие лимиты запросов |
| `AdminApiKey` | `local-admin-key` | Защита `/api/*` |
| `Sync:PollIntervalSeconds` | `30` | Пауза между проходами одного партнёра |
| `Sync:RequestTimeoutSeconds` | `10` | Таймаут HTTP-попытки вместе с ожиданием квоты; deadline gRPC |
| `Sync:MaxPagesPerSweep` | `1000` | Ограничение длины обхода |
| `Sync:MaxApplyAttempts` | `5` | Число попыток gRPC перед DLQ |
| `Sync:InternalGrpcUrl` | `http://localhost:8082` | Адрес внутренней системы |
| `Sync:InternalApiKey` | `local-internal-key` | Ключ внутренней системы |
| `Sync:Partners` | Один партнёр `acme` | `Id`, `BaseUrl`, `ApiKey`, `RequestsPerSecond` |
| `Kafka:BootstrapServers` | `localhost:9092` | Адрес брокера для приложения на хосте |
| `Kafka:RepairTopic` | `partner-sync.repairs.v1` | Команды исправления |
| `Kafka:DeadLetterTopic` | `partner-sync.repairs.dlq.v1` | Сообщения, требующие разбора |
| `Kafka:GroupId` | `partner-sync-apply-v1` | Consumer group |

Допустимый бюджет попыток проверяется при старте: таймауты и задержки должны уложиться в 240 секунд при `max.poll.interval.ms=300000`. Демо-топики создаёт `kafka-init`: три partition, replication factor 1, хранение repairs семь дней, DLQ 30 дней. Публикация outbox хранит команду до подтверждения брокера; после публикации срок хранения Kafka остаётся конечным.

Метрики выпускаются meter `PartnerSync`: `partnersync.sweeps`, `partnersync.orders.inspected`, `partnersync.outbox.published`, `partnersync.repairs.applied`, `partnersync.repairs.dead_letters`. Для экспорта требуется подключить отдельный сборщик. `/health/ready` проверяет доступность зависимостей; ошибки данных и устаревшие результаты ищите в логах и `checkedAt`.

## Документация зависимостей

- [gRPC client factory в ASP.NET Core](https://learn.microsoft.com/en-us/aspnet/core/grpc/clientfactory?view=aspnetcore-10.0)
- [Настройки Kafka producer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerConfig.html)
- [Официальный Docker-образ Kafka](https://kafka.apache.org/41/getting-started/docker/)
- [Npgsql 10.0.0](https://www.nuget.org/packages/Npgsql/10.0.0)
- [Confluent.Kafka 2.15.1](https://www.nuget.org/packages/Confluent.Kafka/2.15.1)
