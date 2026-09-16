import json
import os
import time
import uuid
from datetime import datetime, timezone

import pulsar
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
TOPIC = os.getenv("PULSAR_TOPIC", "persistent://public/default/demo-messages")

app = FastAPI(title="Pulsar Producer")
client = None
producer = None


class MessageRequest(BaseModel):
    message: str = Field(min_length=1, max_length=1000)
    message_type: str = Field(default="demo", min_length=1, max_length=100)


def get_producer():
    global client, producer
    if producer is not None:
        return producer

    last_error = None
    for _ in range(30):
        try:
            client = pulsar.Client(SERVICE_URL)
            producer = client.create_producer(TOPIC)
            return producer
        except Exception as exc:
            last_error = exc
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            client = None
            producer = None
            time.sleep(2)
    raise RuntimeError(f"Unable to connect to Pulsar at {SERVICE_URL}: {last_error}")


@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!doctype html>
<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Pulsar Producer</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;margin:0;background:#0b1020;color:#eef2ff}main{max-width:760px;margin:48px auto;padding:24px}.card{background:#151b2f;border:1px solid #28314f;border-radius:16px;padding:24px}.row{display:flex;gap:12px;flex-wrap:wrap}input,select,button{font:inherit;border-radius:10px;border:1px solid #3a456c;padding:12px}input{flex:1;min-width:240px;background:#0f1528;color:#fff}select{background:#0f1528;color:#fff}button{background:#4f7cff;color:#fff;border:0;font-weight:700;cursor:pointer}.status{margin-top:16px;padding:12px;border-radius:10px;background:#0f1528;white-space:pre-wrap}.muted{color:#a8b1cc}
</style></head><body><main><div class='card'><h1>Apache Pulsar Producer</h1><p class='muted'>Send a message into Pulsar and watch it appear in the consumer dashboard.</p><form id='form'><div class='row'><input id='message' placeholder='Type a message...' required><select id='type'><option value='demo'>demo</option><option value='poison'>poison</option></select><button>Send</button></div></form><div id='status' class='status'>Ready</div></div></main><script>
const form=document.getElementById('form'), status=document.getElementById('status');
form.addEventListener('submit',async(e)=>{e.preventDefault();status.textContent='Sending...';const r=await fetch('/api/messages',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({message:document.getElementById('message').value,message_type:document.getElementById('type').value})});const data=await r.json();status.textContent=JSON.stringify(data,null,2);if(r.ok)document.getElementById('message').value='';});
</script></body></html>
"""


@app.post("/api/messages")
def send_message(request: MessageRequest):
    try:
        envelope = {
            "event_id": str(uuid.uuid4()),
            "type": request.message_type,
            "message": request.message,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        message_id = get_producer().send(json.dumps(envelope).encode("utf-8"))
        return {
            "status": "sent",
            "event": envelope,
            "pulsar_message_id": str(message_id),
            "topic": TOPIC,
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/health")
def health():
    return {"status": "ok", "service_url": SERVICE_URL, "topic": TOPIC}


@app.on_event("shutdown")
def shutdown():
    global producer, client
    if producer is not None:
        producer.close()
    if client is not None:
        client.close()
