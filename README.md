# Apache Pulsar End-to-End PoC

A runnable Apache Pulsar proof of concept with a browser-based Producer UI, a Consumer Dashboard, retry/redelivery, DLQ handling, a smoke test, and an Azure Container Apps deployment path.

## Visual architecture

```text
Browser
  |
  | HTTPS
  v
+----------------------+        Pulsar protocol        +----------------------+
| Producer Web App     | ----------------------------> | Apache Pulsar        |
| POST /api/messages   |                               | standalone broker    |
| UI: send message     |                               | port 6650            |
+----------------------+                               +----------+-----------+
                                                                  |
                                                                  | Shared subscription
                                                                  v
                                                       +----------+-----------+
                                                       | Consumer Web App     |
                                                       | background consumer  |
                                                       | live dashboard       |
                                                       +----------+-----------+
                                                                  |
                                          poison event -> retry -> DLQ
```

## What you can see at runtime

The Producer page lets you type a message and choose either `demo` or `poison`.

The Consumer Dashboard refreshes every second and displays:

- consumed messages
- event IDs
- receive timestamps
- redelivery count
- retry events
- DLQ events

A normal message appears once as `consumed`. A `poison` message appears multiple times as `retry` and then as `dlq`.

## Local visual demo

Prerequisite: Docker with Docker Compose.

```bash
docker compose up --build
```

Open these two browser windows side by side:

- Producer UI: http://localhost:8001
- Consumer Dashboard: http://localhost:8002

Type a message in Producer and click **Send**. Within about a second it appears on the Consumer Dashboard.

To visualize retry/DLQ behavior, choose `poison` and send a message.

Stop everything with:

```bash
docker compose down -v
```

## Azure visual demo

The Azure deployment uses one Azure Container Apps environment:

- `pulsar-broker` - internal TCP ingress on port 6650
- `pulsar-producer` - external HTTPS Producer UI
- `pulsar-consumer` - external HTTPS Consumer Dashboard
- Azure Container Registry - stores the two Python application images

Azure Container Apps supports internal TCP communication between apps in the same environment, so the Pulsar broker does not need to be publicly exposed.

### Deploy

Prerequisites:

- Azure subscription
- Azure CLI
- Docker is not required for the Azure build because `az acr build` builds the images in Azure

Login first:

```bash
az login
```

From the repository root:

```bash
chmod +x azure/deploy.sh
./azure/deploy.sh
```

Optional environment overrides:

```bash
LOCATION=westeurope \
RESOURCE_GROUP=pulsar-poc-rg \
ENVIRONMENT=pulsar-poc-env \
ACR_NAME=<globally-unique-lowercase-name> \
./azure/deploy.sh
```

At the end the script prints two URLs similar to:

```text
Producer UI:        https://pulsar-producer.<region>.azurecontainerapps.io
Consumer Dashboard: https://pulsar-consumer.<region>.azurecontainerapps.io
```

Open both URLs side by side, send a message from Producer, and watch it appear in Consumer Dashboard.

## Automated smoke test

The existing automated smoke test still verifies the messaging semantics directly:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d pulsar
python smoke_test.py
```

Expected result:

```text
E2E PASS: produced, consumed, retried and dead-lettered successfully
```

GitHub Actions runs this verification on every push and pull request.

## Versions

- Apache Pulsar broker: `4.2.4`
- Pulsar Python client: `3.13.0`
- Python: `3.10`
- FastAPI / Uvicorn for browser-facing apps

## Repository layout

```text
.
├── .github/workflows/e2e.yml
├── azure/deploy.sh
├── Dockerfile.producer
├── Dockerfile.consumer
├── docker-compose.yml
├── requirements.txt
├── producer_app.py
├── consumer_app.py
├── producer.py
├── consumer.py
├── smoke_test.py
└── README.md
```

## Production note

This is intentionally a PoC. The Azure example runs Pulsar standalone with one replica and no authentication so the message flow is easy to demonstrate. A production Pulsar platform should use an appropriate clustered deployment, persistent storage, authentication/TLS, authorization, monitoring, capacity testing, schema governance, and an HA/DR design.
