import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING", "")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "demo-events")
CONSUMER_GROUP = os.getenv("EVENTHUB_CONSUMER_GROUP", "demo-consumers")
BLOB_CONNECTION_STRING = os.getenv("BLOB_STORAGE_CONNECTION_STRING", "")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER_NAME", "eventhub-checkpoints")
CONSUMER_ID = os.getenv("CONSUMER_ID", "eventhub-consumer")

app = FastAPI(title=f"Event Hubs Consumer {CONSUMER_ID}")
_events = deque(maxlen=200)
_owned = set()
_lock = threading.Lock()
_stop = threading.Event()
_worker = None


def add_event(payload, partition_id, sequence_number, offset):
    with _lock:
        _events.appendleft({
            **payload,
            "consumer_id": CONSUMER_ID,
            "partition_id": partition_id,
            "sequence_number": sequence_number,
            "offset": str(offset),
            "received_at": datetime.now(timezone.utc).isoformat(),
        })


def consume_loop():
    if not CONNECTION_STRING or not BLOB_CONNECTION_STRING:
        return
    checkpoint_store = BlobCheckpointStore.from_connection_string(BLOB_CONNECTION_STRING, BLOB_CONTAINER)
    client = EventHubConsumerClient.from_connection_string(
        CONNECTION_STRING,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store,
    )

    def on_partition_initialize(partition_context):
        with _lock:
            _owned.add(partition_context.partition_id)

    def on_partition_close(partition_context, reason):
        with _lock:
            _owned.discard(partition_context.partition_id)

    def on_event(partition_context, event):
        try:
            payload = json.loads(event.body_as_str(encoding="UTF-8"))
        except Exception:
            payload = {"event_id": "-", "message": event.body_as_str(encoding="UTF-8")}
        add_event(payload, partition_context.partition_id, event.sequence_number, event.offset)
        partition_context.update_checkpoint(event)

    def on_error(partition_context, error):
        add_event({"event_id": "-", "message": f"ERROR: {error}"}, getattr(partition_context, "partition_id", "-"), -1, "-")

    try:
        client.receive(
            on_event=on_event,
            on_error=on_error,
            on_partition_initialize=on_partition_initialize,
            on_partition_close=on_partition_close,
            starting_position="-1",
        )
    finally:
        client.close()
        checkpoint_store.close()


@app.on_event("startup")
def startup():
    global _worker
    _worker = threading.Thread(target=consume_loop, daemon=True)
    _worker.start()


@app.get("/", response_class=HTMLResponse)
def index():
    return f"""
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>{CONSUMER_ID}</title>
<style>body{{font-family:system-ui;margin:0;background:#0b1020;color:#eef2ff}}main{{max-width:1050px;margin:34px auto;padding:20px}}.top{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap}}.card{{background:#151b2f;border:1px solid #28314f;border-radius:14px;padding:16px}}.muted{{color:#a8b1cc}}.big{{font-size:28px;font-weight:800}}table{{width:100%;border-collapse:collapse;margin-top:16px;background:#11172a}}th,td{{padding:10px;border-bottom:1px solid #242d49;text-align:left}}.mono{{font-family:ui-monospace,monospace;font-size:12px}}</style></head><body><main><div class='top'><div><h1>{CONSUMER_ID}</h1><div class='muted'>Event Hubs consumer group: {CONSUMER_GROUP}</div></div><div class='card'><div class='muted'>Owned partitions</div><div id='owned' class='big'>-</div></div></div><div class='card' style='margin-top:16px'><div class='muted'>With 2 partitions and 3 consumers, one consumer should normally own no partition.</div><table><thead><tr><th>Message</th><th>Partition</th><th>Sequence</th><th>Event ID</th><th>Received</th></tr></thead><tbody id='rows'></tbody></table></div></main><script>
const esc=s=>String(s??'').replace(/[&<>'"]/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}}[c]));async function refresh(){{const r=await fetch('/api/state');const x=await r.json();owned.textContent=x.owned_partitions.length?x.owned_partitions.join(', '):'none';rows.innerHTML=x.events.map(e=>`<tr><td>${{esc(e.message)}}</td><td>${{esc(e.partition_id)}}</td><td>${{esc(e.sequence_number)}}</td><td class='mono'>${{esc(e.event_id)}}</td><td class='mono'>${{esc(e.received_at)}}</td></tr>`).join('')}}refresh();setInterval(refresh,1000);
</script></body></html>
"""


@app.get("/api/state")
def state():
    with _lock:
        return {"consumer_id": CONSUMER_ID, "owned_partitions": sorted(_owned), "events": list(_events)}


@app.get("/health")
def health():
    return {"status": "ok", "consumer_id": CONSUMER_ID, "configured": bool(CONNECTION_STRING and BLOB_CONNECTION_STRING)}
