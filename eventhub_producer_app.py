import json
import os
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING", "")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "demo-events")

app = FastAPI(title="Event Hubs Producer")


class SendRequest(BaseModel):
    message: str = Field(min_length=1, max_length=1000)
    count: int = Field(default=12, ge=1, le=200)
    partition_key: str | None = Field(default=None, max_length=100)


@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Event Hubs Producer</title>
<style>body{font-family:system-ui;margin:0;background:#0b1020;color:#eef2ff}main{max-width:820px;margin:40px auto;padding:20px}.card{background:#151b2f;border:1px solid #28314f;border-radius:16px;padding:22px}.row{display:flex;gap:10px;flex-wrap:wrap}input,button{font:inherit;padding:12px;border-radius:10px;border:1px solid #3a456c}input{background:#0f1528;color:#fff}#message{flex:2;min-width:240px}#count{width:90px}#key{flex:1;min-width:140px}button{background:#4f7cff;color:#fff;border:0;font-weight:700}.muted{color:#a8b1cc}.status{margin-top:14px;padding:12px;background:#0f1528;border-radius:10px;white-space:pre-wrap}</style></head><body><main><div class='card'><h1>Azure Event Hubs Producer</h1><p class='muted'>Send a burst. With 2 partitions and 3 consumer apps, only two consumers should own partitions at steady state.</p><form id='f'><div class='row'><input id='message' value='event-hubs-demo' required><input id='count' type='number' value='12' min='1' max='200'><input id='key' placeholder='partition key (optional)'><button>Send burst</button></div></form><div id='s' class='status'>Ready</div></div></main><script>f.addEventListener('submit',async e=>{e.preventDefault();s.textContent='Sending...';const r=await fetch('/api/send',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({message:message.value,count:Number(count.value),partition_key:key.value||null})});s.textContent=JSON.stringify(await r.json(),null,2)});</script></body></html>
"""


@app.post("/api/send")
def send(req: SendRequest):
    if not CONNECTION_STRING:
        raise HTTPException(503, "EVENTHUB_CONNECTION_STRING is not configured")
    client = EventHubProducerClient.from_connection_string(CONNECTION_STRING, eventhub_name=EVENTHUB_NAME)
    ids = []
    try:
        events = []
        for i in range(1, req.count + 1):
            payload = {
                "event_id": str(uuid.uuid4()),
                "message": f"{req.message} #{i}",
                "sequence": i,
                "sent_at": datetime.now(timezone.utc).isoformat(),
            }
            ids.append(payload["event_id"])
            events.append(EventData(json.dumps(payload)))
        client.send_batch(events, partition_key=req.partition_key)
        return {"status": "sent", "count": req.count, "eventhub": EVENTHUB_NAME, "partition_key": req.partition_key, "event_ids": ids}
    finally:
        client.close()


@app.get("/health")
def health():
    return {"status": "ok", "configured": bool(CONNECTION_STRING), "eventhub": EVENTHUB_NAME}
