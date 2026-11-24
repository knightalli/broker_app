import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import pika
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
TASKS_QUEUE = os.getenv("TASKS_QUEUE", "tasks")
RESULTS_QUEUE = os.getenv("RESULTS_QUEUE", "results")

app = FastAPI(title="Service A — Jobs API")

jobs: dict[str, dict] = {}
lock = threading.Lock()


def _conn() -> pika.BlockingConnection:
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat=30)
    )


def publish_task(job_id: str, payload: dict) -> None:
    conn = _conn()
    try:
        ch = conn.channel()
        ch.queue_declare(queue=TASKS_QUEUE, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=TASKS_QUEUE,
            body=json.dumps({"job_id": job_id, "payload": payload}),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
    finally:
        conn.close()


def _on_result(ch, method, _props, body: bytes) -> None:
    try:
        event = json.loads(body)
        job_id = event["job_id"]
        with lock:
            job = jobs.setdefault(job_id, {"id": job_id})
            job["status"] = "DONE"
            job["result"] = event.get("result")
            job["completed_at"] = event.get("completed_at")
            job["redelivered"] = event.get("redelivered")
        print(f"[A] JobCompleted received for {job_id} (redelivered={method.redelivered})")
        ch.basic_ack(method.delivery_tag)
    except Exception as exc:
        print(f"[A] bad result message, rejecting: {exc}")
        ch.basic_reject(method.delivery_tag, requeue=False)


def consume_results() -> None:
    while True:
        try:
            conn = _conn()
            ch = conn.channel()
            ch.queue_declare(queue=RESULTS_QUEUE, durable=True)
            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(queue=RESULTS_QUEUE, on_message_callback=_on_result)
            print(f"[A] consuming '{RESULTS_QUEUE}'")
            ch.start_consuming()
        except Exception as exc:
            print(f"[A] results consumer down ({exc}), retry in 2s")
            time.sleep(2)


@app.on_event("startup")
def startup() -> None:
    threading.Thread(target=consume_results, daemon=True).start()


@app.post("/jobs", status_code=202)
def create_job(payload: dict = {}) -> JSONResponse:
    job_id = str(uuid.uuid4())
    try:
        publish_task(job_id, payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"broker unavailable: {exc}")

    with lock:
        jobs.setdefault(
            job_id,
            {
                "id": job_id,
                "status": "ACCEPTED",
                "result": None,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": None,
                "redelivered": None,
            },
        )
        job = jobs[job_id]
    return JSONResponse(status_code=202, content=job)


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse("static/index.html")


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict:
    with lock:
        job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job
