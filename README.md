# broker_app — двухсервисный демо с RabbitMQ

Асинхронная обработка задач через брокер сообщений:

```
клиент ──POST /jobs──▶ service-a ──▶ queue: tasks ──▶ service-b
                                                          │ 2–3 сек "работа"
клиент ◀──GET /jobs/{id}── service-a ◀── queue: results ◀──┘ JobCompleted
```

| Компонент | Стек | Роль |
|---|---|---|
| `service-a` | FastAPI + pika | REST API приёма задач и чтения статуса (состояние в памяти) |
| `service-b` | Python + pika | воркер: слушает `tasks`, «медленно» обрабатывает, публикует событие в `results` |
| `rabbitmq` | rabbitmq:3.13-management | брокер + management UI на `:15672` |

Контракт между сервисами — очередь и JSON-сообщения, а не вызов: сервисы ничего не знают друг о друге.

## Запуск

```bash
docker compose up --build
```

Одна команда поднимает все три контейнера. `service-a` и `service-b` стартуют только после healthcheck RabbitMQ.

## Веб-интерфейс

Открыть http://localhost:8000 — простая страница для проверки без curl:

- **«Отправить задачу»** — POST /jobs, видно время ответа 202 в мс; карточка задачи сама опрашивает GET /jobs/{id} раз в секунду и переключается в DONE с результатом.
- **«Проверить at-least-once»** — отправляет задачу с флагом `"chaos":"crash"`: воркер B завершает процесс до publish+ack, контейнер перезапускается (`restart: unless-stopped`), сообщение редоставляется, и результат приходит с пометкой `redelivered`. В логах `docker compose logs -f service-b` видно `CHAOS: crashing ...` и повторный `processing job ...`.

## Сценарий использования (curl)

**1. Отправить задачу — ответ приходит мгновенно:**

```bash
curl -i -X POST localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"topic": "demo", "text": "hello broker"}'
```

Ожидаемый вывод (~30 мс):

```
HTTP/1.1 202 Accepted

{"id":"19c7a0c7-...","status":"ACCEPTED","result":null,
 "created_at":"2026-11-23T17:18:51.98...","completed_at":null}
```

**2. Через ~3 секунды запросить результат:**

```bash
curl localhost:8000/jobs/19c7a0c7-...   # подставить id из шага 1
```

```json
{"id":"19c7a0c7-...","status":"DONE",
 "result":{"summary":"processed payload with 41 bytes","echo":{"topic":"demo","text":"hello broker"}},
 "created_at":"2026-11-23T17:18:51.98...","completed_at":"2026-11-23T17:18:54.31..."}
```

Пока `service-b` не обработал задачу, `GET` возвращает `{"status":"ACCEPTED", ...}`.

## Очереди в management UI

Открыть http://localhost:15672 (логин/пароль: `guest` / `guest`) → вкладка **Queues and Streams**. Там видны обе durable-очереди:

- `tasks` — задачи от A к B;
- `results` — события `JobCompleted` от B к A.

В колонках Ready/Unacked/Total видно движение сообщений; на вкладке **Connections** — подключения обоих сервисов.

## Почему POST отвечает 202, а не 200

`202 Accepted` означает: запрос принят, но обработка ещё не завершена и будет выполняться асинхронно. Клиент сразу получает `id` задачи и дальше опрашивает `GET /jobs/{id}`. Это честнее, чем держать HTTP-соединение 3 секунды, и не блокирует API под долгую работу.

## Что будет, если service-b упал в середине обработки

Гарантия **at-least-once**:

- очередь `tasks` объявлена `durable`, сообщения публикуются persistent;
- `service-b` подтверждает сообщение вручную (`basic_ack`) **после** публикации результата в `results`;
- если B падает до ack, брокер возвращает сообщение в очередь и доставляет его повторно (другому экземпляру B или тому же после рестарта) с флагом `redelivered=True`;
- симметрично на стороне A: `results` — durable + manual ack.

## Защита от дублей

At-least-once означает, что дубликаты возможны (например, B опубликовал результат, но упал до ack). От них защищены обе стороны:

- **service-b** — идемпотентность по `job_id`: словарь `processed` хранит уже сформированные события; при повторной доставке работа не выполняется заново, а публикуется то же самое событие `JobCompleted`.
- **service-a** — применение события идемпотентно: `DONE + result` просто перезаписывается теми же значениями.

## Бонус: воспроизвести at-least-once вручную

Проще всего — красная кнопка в UI (см. выше). Из терминала то же самое даёт `"chaos":"crash"`:

```bash
curl -X POST localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"topic":"chaos","chaos":"crash"}'
# B упадёт, перезапустится, сообщение redeliver'ится, job всё равно станет DONE
```

Или «убить» контейнер вручную:

```bash
# 1. отправить задачу
curl -X POST localhost:8000/jobs -H "Content-Type: application/json" -d '{"topic":"kill-test"}'

# 2. в течение ~1–2 сек убить воркер, пока он "работает"
docker compose kill service-b

# 3. поднять обратно — сообщение redeliver'ится, задача дообработается
docker compose start service-b
docker logs broker-service-b   # увидите повторный "processing job <id>"
```

`GET /jobs/{id}` после этого всё равно вернёт `DONE` — сообщение не потерялось.

Остановить всё: `docker compose down`.

## Структура

```
service-a/   FastAPI + pika: POST/GET /jobs, консьюмер results, веб-UI на /
service-b/   pika-воркер: консьюмер tasks, публикация JobCompleted, chaos-режим
docker-compose.yml
```
