import json
import os
import socket
import threading
import time
from collections import deque
from datetime import datetime, timezone

import pulsar
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
TOPIC = os.getenv("PULSAR_TOPIC", "persistent://public/default/demo-messages")
DLQ_TOPIC = os.getenv("PULSAR_DLQ_TOPIC", "persistent://public/default/demo-messages-dlq")
SUBSCRIPTION = os.getenv("PULSAR_SUBSCRIPTION", "demo-consumers")
DLQ_SUBSCRIPTION = os.getenv("PULSAR_DLQ_SUBSCRIPTION", "demo-consumers-dlq")
CONSUMER_ID = os.getenv("CONSUMER_ID", socket.gethostname())
MAX_REDELIVERIES = int(os.getenv("PULSAR_MAX_REDELIVERIES", "3"))

app = FastAPI(title=f"Pulsar Consumer Dashboard - {CONSUMER_ID}")
_events = deque(maxlen=300)
_lock = threading.Lock()
_stop = threading.Event()
_worker = None


def record(status: str, payload: dict, redelivery_count: int = 0):
    with _lock:
        _events.appendleft({
            "consumer_id": CONSUMER_ID,
            "status": status,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "redelivery_count": redelivery_count,
            **payload,
        })


def decode(msg):
    try:
        return json.loads(msg.data().decode("utf-8"))
    except Exception:
        return {"event_id": str(msg.message_id()), "type": "invalid", "message": msg.data().decode("utf-8", errors="replace")}


def consume_loop():
    while not _stop.is_set():
        client = None
        main_consumer = None
        dlq_consumer = None
        try:
            client = pulsar.Client(SERVICE_URL)
            main_consumer = client.subscribe(
                TOPIC,
                subscription_name=SUBSCRIPTION,
                consumer_type=pulsar.ConsumerType.Shared,
                consumer_name=CONSUMER_ID,
                initial_position=pulsar.InitialPosition.Earliest,
                negative_ack_redelivery_delay_ms=1000,
                dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(
                    max_redeliver_count=MAX_REDELIVERIES,
                    dead_letter_topic=DLQ_TOPIC,
                    initial_subscription_name=f"{SUBSCRIPTION}-dlq-initial",
                ),
            )
            dlq_consumer = client.subscribe(
                DLQ_TOPIC,
                subscription_name=DLQ_SUBSCRIPTION,
                consumer_type=pulsar.ConsumerType.Shared,
                consumer_name=f"{CONSUMER_ID}-dlq",
                initial_position=pulsar.InitialPosition.Earliest,
            )
            record("connected", {"event_id": "-", "type": "system", "message": f"{CONSUMER_ID} joined shared subscription {SUBSCRIPTION}"})

            while not _stop.is_set():
                try:
                    msg = main_consumer.receive(timeout_millis=300)
                except pulsar.Timeout:
                    msg = None

                if msg is not None:
                    payload = decode(msg)
                    redelivery_count = msg.redelivery_count()
                    if payload.get("type") == "poison":
                        record("retry", payload, redelivery_count)
                        main_consumer.negative_acknowledge(msg)
                    else:
                        record("consumed", payload, redelivery_count)
                        main_consumer.acknowledge(msg)

                try:
                    dlq_msg = dlq_consumer.receive(timeout_millis=100)
                except pulsar.Timeout:
                    dlq_msg = None

                if dlq_msg is not None:
                    payload = decode(dlq_msg)
                    record("dlq", payload, dlq_msg.redelivery_count())
                    dlq_consumer.acknowledge(dlq_msg)

        except Exception as exc:
            record("connection-error", {"event_id": "-", "type": "system", "message": str(exc)})
            time.sleep(2)
        finally:
            for obj in (main_consumer, dlq_consumer, client):
                if obj is not None:
                    try:
                        obj.close()
                    except Exception:
                        pass


@app.on_event("startup")
def startup():
    global _worker
    _worker = threading.Thread(target=consume_loop, daemon=True)
    _worker.start()


@app.on_event("shutdown")
def shutdown():
    _stop.set()
    if _worker is not None:
        _worker.join(timeout=5)


@app.get("/", response_class=HTMLResponse)
def index():
    return f"""
<!doctype html>
<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Pulsar Consumer {CONSUMER_ID}</title>
<style>
body{{font-family:system-ui,-apple-system,sans-serif;margin:0;background:#0b1020;color:#eef2ff}}main{{max-width:1150px;margin:36px auto;padding:20px}}.top{{display:flex;justify-content:space-between;align-items:end;gap:16px;flex-wrap:wrap}}.muted{{color:#a8b1cc}}.badge{{display:inline-block;padding:6px 10px;border-radius:999px;background:#24345d;font-weight:800}}.stats{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:20px 0}}.card{{background:#151b2f;border:1px solid #28314f;border-radius:14px;padding:16px}}.value{{font-size:28px;font-weight:800}}.table{{overflow:auto;border:1px solid #28314f;border-radius:14px}}table{{width:100%;border-collapse:collapse;background:#11172a}}th,td{{text-align:left;padding:12px;border-bottom:1px solid #242d49;vertical-align:top}}th{{position:sticky;top:0;background:#151b2f}}.pill{{display:inline-block;padding:4px 9px;border-radius:999px;font-size:12px;font-weight:700}}.consumed{{background:#123d2c}}.retry{{background:#533f0e}}.dlq{{background:#54202a}}.connected{{background:#233b58}}.connection-error{{background:#4a263d}}.mono{{font-family:ui-monospace,SFMono-Regular,monospace;font-size:12px}}@media(max-width:700px){{.stats{{grid-template-columns:1fr 1fr}}th:nth-child(4),td:nth-child(4){{display:none}}}}
</style></head><body><main><div class='top'><div><h1>Pulsar Shared Consumer <span class='badge'>{CONSUMER_ID}</span></h1><div class='muted'>Subscription: <b>{SUBSCRIPTION}</b>. Open all consumer dashboards side by side and send a batch.</div></div><div class='muted'>Refreshes every second</div></div><div class='stats'><div class='card'><div class='muted'>Consumed here</div><div id='consumed' class='value'>0</div></div><div class='card'><div class='muted'>Retries here</div><div id='retry' class='value'>0</div></div><div class='card'><div class='muted'>DLQ here</div><div id='dlq' class='value'>0</div></div><div class='card'><div class='muted'>Events shown</div><div id='total' class='value'>0</div></div></div><div class='table'><table><thead><tr><th>Status</th><th>Message</th><th>Sequence</th><th>Event ID</th><th>Redelivery</th><th>Time</th></tr></thead><tbody id='rows'></tbody></table></div></main><script>
async function refresh(){{try{{const r=await fetch('/api/messages');const items=await r.json();const counts={{consumed:0,retry:0,dlq:0}};for(const x of items){{if(counts[x.status]!==undefined)counts[x.status]++;}}document.getElementById('consumed').textContent=counts.consumed;document.getElementById('retry').textContent=counts.retry;document.getElementById('dlq').textContent=counts.dlq;document.getElementById('total').textContent=items.length;document.getElementById('rows').innerHTML=items.map(x=>`<tr><td><span class="pill ${{x.status}}">${{x.status}}</span></td><td>${{escapeHtml(x.message||'')}}</td><td>${{x.sequence||''}}</td><td class="mono">${{escapeHtml(x.event_id||'')}}</td><td>${{x.redelivery_count||0}}</td><td class="mono">${{escapeHtml(x.received_at||'')}}</td></tr>`).join('');}}catch(e){{console.error(e)}}}}
function escapeHtml(s){{return String(s).replace(/[&<>'"]/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}}[c]));}}refresh();setInterval(refresh,1000);
</script></body></html>
"""


@app.get("/api/messages")
def messages():
    with _lock:
        return list(_events)


@app.get("/health")
def health():
    return {"status": "ok", "consumer_id": CONSUMER_ID, "service_url": SERVICE_URL, "topic": TOPIC, "subscription": SUBSCRIPTION, "dlq_topic": DLQ_TOPIC}
