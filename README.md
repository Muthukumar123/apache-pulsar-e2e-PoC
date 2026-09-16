# Apache Pulsar End-to-End PoC

A runnable Apache Pulsar proof of concept with a browser-based producer, multiple consumer dashboards, Shared subscription load distribution, acknowledgements, negative acknowledgements, retry/redelivery, DLQ handling, Azure Container Apps deployment, and an automated smoke test.

## Why no Dapr?

Dapr is not required for this PoC. The goal is to demonstrate Apache Pulsar's native producer, subscription, acknowledgement, retry, and DLQ behavior directly. Adding Dapr pub/sub would insert another abstraction between the application and Pulsar and make it harder to see the Pulsar semantics we are testing.

Dapr can be added later for service invocation, tracing, secrets, or a standardized application integration layer, but it is intentionally not part of the core messaging path here.

## Shared subscription architecture

```text
                                   same subscription: demo-consumers

+----------------+      +------------------+       +-------------------+
| Producer UI    | ---> | Apache Pulsar    | ----> | Consumer 1        |
| batch send     |      | demo-messages    |  \    | consumer-1        |
+----------------+      +------------------+   +-> | Consumer 2        |
                                              \   | consumer-2        |
                                               +> | Consumer 3        |
                                                  | consumer-3        |
                                                  +-------------------+
```

All three consumers attach to the same topic with:

```text
subscription_name = demo-consumers
subscription_type = Shared
```

Pulsar distributes individual messages among the active consumers. A given message goes to one consumer in the Shared subscription. Shared subscriptions do not provide ordering guarantees.

## Local visual demo

Start everything:

```bash
docker compose down -v
docker compose up --build
```

Open these four browser windows side by side:

| App | URL |
|---|---|
| Producer | http://localhost:8001 |
| Consumer 1 | http://localhost:8002 |
| Consumer 2 | http://localhost:8003 |
| Consumer 3 | http://localhost:8004 |

In Producer enter a message such as `shared-demo`, set **count = 30**, keep type `demo`, and click **Send**.

Each generated message contains a sequence number. Watch the three dashboards: the messages will be split between `consumer-1`, `consumer-2`, and `consumer-3` because they all use the same `demo-consumers` Shared subscription.

To demonstrate retry and DLQ behavior, send one or more `poison` messages. Consumers negatively acknowledge poison messages; Pulsar redelivers them and moves them to the DLQ after the configured redelivery limit.

## Azure visual demo

The Azure deployment uses Azure Container Apps:

```text
Producer UI (external HTTPS)
        |
        v
Pulsar broker (internal TCP 6650)
        |
        +------> Consumer 1 dashboard (external HTTPS)
        +------> Consumer 2 dashboard (external HTTPS)
        +------> Consumer 3 dashboard (external HTTPS)

All consumers: PULSAR_SUBSCRIPTION=demo-consumers
```

Deploy:

```bash
az login
chmod +x azure/deploy.sh
./azure/deploy.sh
```

The script prints four browser URLs: Producer, Consumer 1, Consumer 2, and Consumer 3. Open them side by side, send a batch from Producer, and observe the Shared subscription distribution visually.

## Automated E2E smoke test

The repository also retains the deterministic CLI smoke test:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python smoke_test.py
```

Expected final output:

```text
E2E PASS: produced, consumed, retried and dead-lettered successfully
```

## Versions

- Apache Pulsar broker: `4.2.4`
- Pulsar Python client: `3.13.0`
- Python: `3.10`
- FastAPI/Uvicorn for the demo web apps

## Repository layout

```text
.
├── .github/workflows/e2e.yml
├── azure/deploy.sh
├── Dockerfile.producer
├── Dockerfile.consumer
├── docker-compose.yml
├── producer_app.py
├── consumer_app.py
├── producer.py
├── consumer.py
├── smoke_test.py
├── requirements.txt
└── README.md
```

## Production considerations

This repository intentionally uses a standalone broker and no authentication so the messaging semantics are easy to inspect. A production design should use an appropriately sized Pulsar cluster, durable storage/metadata, TLS and authentication/authorization, explicit tenant/namespace policies, observability, schema governance, capacity/SLO testing, and disaster recovery/geo-replication where required.
