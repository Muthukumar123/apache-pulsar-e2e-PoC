# Apache Pulsar End-to-End PoC

A runnable proof of concept for Apache Pulsar covering local broker startup, producing and consuming JSON events, acknowledgements, negative acknowledgements, retry/redelivery, dead-letter routing, and an automated end-to-end smoke test.

## Architecture

```text
+----------------+       persistent://public/default/poc-events       +----------------+
|   Producer     | ----------------------------------------------->   | Apache Pulsar  |
| producer.py    |                                                    | standalone     |
+----------------+                                                    +-------+--------+
                                                                             |
                                                                             | Shared subscription
                                                                             v
                                                                     +-------+--------+
                                                                     | Consumer       |
                                                                     | consumer.py    |
                                                                     +-------+--------+
                                                                             |
                                                      processing failure      | negative ack
                                                                             v
                                                                    retry/redelivery
                                                                             |
                                                          max redeliveries    v
                                                                     +-------+--------+
                                                                     | Dead-letter    |
                                                                     | topic (DLQ)    |
                                                                     +----------------+
```

## What this PoC proves

- Pulsar standalone runs locally in Docker.
- A Python producer publishes structured JSON events.
- A Python consumer uses a durable Shared subscription.
- Successful messages are acknowledged explicitly.
- Failed/poison messages are negatively acknowledged and redelivered.
- Messages exceeding the configured redelivery limit are routed to a DLQ.
- `smoke_test.py` validates the complete producer -> broker -> consumer -> retry -> DLQ path.
- GitHub Actions runs the same end-to-end smoke test on every push/PR.

## Versions

- Apache Pulsar broker: `4.2.4`
- Pulsar Python client: `3.13.0`
- Python: `3.10`

## Prerequisites

- Docker with Docker Compose
- Python 3.10

## Run the full E2E test

```bash
docker compose up -d
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python smoke_test.py
```

Expected final output:

```text
E2E PASS: produced, consumed, retried and dead-lettered successfully
```

Stop Pulsar:

```bash
docker compose down -v
```

## Run producer and consumer manually

Terminal 1:

```bash
docker compose up -d
```

Terminal 2:

```bash
source .venv/bin/activate
python consumer.py
```

Terminal 3:

```bash
source .venv/bin/activate
python producer.py --count 5
```

To demonstrate retry and DLQ behavior, publish a poison event:

```bash
python producer.py --count 3 --include-poison
```

The consumer deliberately negatively acknowledges events whose `type` is `poison`. After three redeliveries Pulsar moves the event to `persistent://public/default/poc-events-dlq`.

## Pulsar endpoints

| Purpose | Endpoint |
|---|---|
| Binary protocol | `pulsar://localhost:6650` |
| Admin / HTTP | `http://localhost:8080` |
| Topic | `persistent://public/default/poc-events` |
| Subscription | `poc-workers` |
| DLQ | `persistent://public/default/poc-events-dlq` |

## Repository layout

```text
.
├── .github/workflows/e2e.yml
├── docker-compose.yml
├── requirements.txt
├── producer.py
├── consumer.py
├── smoke_test.py
└── README.md
```

## Production considerations

This repository intentionally uses a standalone broker and no authentication so the messaging semantics are easy to inspect. A production design should use a multi-broker cluster, BookKeeper/Oxia-backed metadata and storage appropriate to the chosen Pulsar deployment, TLS/authentication and authorization, explicit tenant/namespace policies, observability, capacity/SLO testing, schema governance, disaster recovery/geo-replication where required, and infrastructure-as-code.
