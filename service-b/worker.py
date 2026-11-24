import json
import os
import random
import sys
import time
from datetime import datetime, timezone

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
TASKS_QUEUE = os.getenv("TASKS_QUEUE", "tasks")
RESULTS_QUEUE = os.getenv("RESULTS_QUEUE", "results")

# Idempotency: job_id -> already produced JobCompleted event.
# In-memory is enough for the demo; a real system would persist this.
processed: dict[str, dict] = {}


def _do_work(job_id: str, payload: dict) -> dict:
    delay = random.uniform(2.0, 3.0)
    print(f"[B] processing job {job_id}, sleeping {delay:.2f}s ...", flush=True)
    time.sleep(delay)  # slow stub instead of real work
    return {
        "job_id": job_id,
        "result": {
            "summary": f"processed payload with {len(json.dumps(payload))} bytes",
            "echo": payload,
        },
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


def on_task(ch, method, _props, body: bytes) -> None:
    msg = json.loads(body)
    job_id = msg["job_id"]
    payload = msg.get("payload", {})

    if job_id in processed:
        # Redelivery after a crash between publish and ack:
        # don't redo the work, just re-publish the same fact.
        event = processed[job_id]
        print(f"[B] duplicate delivery of {job_id} (redelivered={method.redelivered}), "
              f"re-publishing stored result", flush=True)
    else:
        event = _do_work(job_id, payload)
        event["redelivered"] = method.redelivered
        processed[job_id] = event

        if payload.get("chaos") == "crash" and not method.redelivered:
            # Chaos demo: die mid-flight, before publish+ack. The task stays
            # unacked -> RabbitMQ requeues it -> the restarted worker picks it
            # up again (at-least-once, redelivered=True).
            print(f"[B] CHAOS: crashing on {job_id} before publish+ack", flush=True)
            sys.stdout.flush()
            os._exit(1)

    ch.basic_publish(
        exchange="",
        routing_key=RESULTS_QUEUE,
        body=json.dumps(event),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )
    print(f"[B] JobCompleted published for {job_id}", flush=True)
    # Ack AFTER the result is published -> at-least-once.
    # Crash before this line => task is requeued and redelivered.
    ch.basic_ack(method.delivery_tag)


def main() -> None:
    while True:
        try:
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat=30)
            )
            ch = conn.channel()
            ch.queue_declare(queue=TASKS_QUEUE, durable=True)
            ch.queue_declare(queue=RESULTS_QUEUE, durable=True)
            ch.basic_qos(prefetch_count=1)  # one task at a time
            ch.basic_consume(queue=TASKS_QUEUE, on_message_callback=on_task)
            print(f"[B] consuming '{TASKS_QUEUE}'", flush=True)
            ch.start_consuming()
        except Exception as exc:
            print(f"[B] consumer down ({exc}), retry in 2s", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    main()
