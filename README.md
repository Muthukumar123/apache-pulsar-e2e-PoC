# Messaging Behavior Lab: Apache Pulsar vs Azure Event Hubs

This repository is a visual PoC for understanding how Apache Pulsar and Azure Event Hubs scale consumers differently.

It contains browser-based producers and consumer dashboards so you can send the same burst of messages into both systems and watch how work is distributed.

## What you are learning

### Pulsar Shared subscription

```text
Producer
   |
   v
Pulsar topic
   |
   | subscription = demo-consumers
   | type = Shared
   |
   +---- message 1 ---> consumer-1
   +---- message 2 ---> consumer-2
   +---- message 3 ---> consumer-3
   +---- message 4 ---> consumer-1
```

All three consumers can be active on the same subscription. Pulsar dispatches individual messages among them. Shared subscriptions do not guarantee ordering.

### Azure Event Hubs consumer group

The comparison deployment deliberately creates **2 partitions and 3 consumers**:

```text
Event Hub: demo-events

Partition 0 ----------> eventhub-consumer-1
Partition 1 ----------> eventhub-consumer-2
                         eventhub-consumer-3 -> no partition / idle

consumer group = demo-consumers
```

Event Hubs distributes **partition ownership** across consumer instances. With two partitions, only two consumers can own work at steady state. The third consumer waits until a partition becomes available.

The three Event Hubs consumers share one Azure Blob checkpoint store, which is how the Python `EventHubConsumerClient` coordinates load balancing and persisted checkpoints.

## Why no Dapr?

Dapr is intentionally not in the core path. The point of this lab is to expose native Pulsar and Event Hubs behavior. Dapr can be added later for service invocation, tracing, secrets, or a common pub/sub abstraction, but that would hide some of the differences this lab is designed to demonstrate.

## Experiment 1: Pulsar Shared subscription locally

```bash
docker compose down -v
docker compose up --build
```

Open:

| App | URL |
|---|---|
| Pulsar Producer | http://localhost:8001 |
| Pulsar Consumer 1 | http://localhost:8002 |
| Pulsar Consumer 2 | http://localhost:8003 |
| Pulsar Consumer 3 | http://localhost:8004 |

Send a burst of 30 messages from the producer. Watch all three dashboards. You should see different messages appear on different consumers, even though all three use the same `demo-consumers` Shared subscription.

Send a `poison` message to see negative acknowledgement, redelivery, and DLQ behavior.

## Experiment 2: Pulsar vs Azure Event Hubs in Azure

Deploy the complete comparison lab:

```bash
az login
chmod +x azure/deploy-comparison.sh
./azure/deploy-comparison.sh
```

The script creates:

- Azure Container Apps environment
- internal Apache Pulsar broker
- Pulsar producer UI
- three separate Pulsar consumer dashboards
- Azure Event Hubs namespace
- Event Hub `demo-events` with exactly 2 partitions
- consumer group `demo-consumers`
- Azure Storage checkpoint container
- Event Hubs producer UI
- three separate Event Hubs consumer dashboards

The script prints all eight browser URLs when deployment is complete.

## How to run the comparison

### Step A — Pulsar

Open the Pulsar Producer and all three Pulsar consumer dashboards.

Send 30 messages.

Observe:

```text
consumer-1 -> some messages
consumer-2 -> some messages
consumer-3 -> some messages
```

The important observation is not a perfect 10/10/10 split. The important observation is that all three consumers can actively participate in one Shared subscription.

### Step B — Event Hubs

Open the Event Hubs Producer and all three Event Hubs consumer dashboards.

Wait for partition balancing, then look at the **Owned partitions** card on every consumer.

With 2 partitions and 3 consumers, you should see something like:

```text
eventhub-consumer-1 -> partition 0
eventhub-consumer-2 -> partition 1
eventhub-consumer-3 -> none
```

The exact consumer-to-partition assignment can change, but there can only be two active partition owners because the Event Hub has two partitions.

Send 30 messages.

Each consumer dashboard shows:

- message text
- partition ID
- sequence number
- event ID
- receive timestamp

This makes the partition model visible instead of hidden in application logs.

## The architectural difference to remember

| Question | Apache Pulsar Shared | Azure Event Hubs |
|---|---|---|
| What is distributed? | Individual messages | Partitions |
| Scaling unit | Consumer on a subscription | Partition ownership |
| 3 consumers, 1 non-partitioned topic | All 3 can process | Not an Event Hubs model |
| 3 consumers, 2 Event Hubs partitions | N/A | 2 active owners, 1 idle |
| Failure handling | ACK/NACK, redelivery, DLQ policies | Offset/checkpoint based processing |
| Ordering | Shared: no ordering guarantee | Ordered within a partition |
| Independent readers | Separate Pulsar subscriptions | Separate Event Hubs consumer groups |

## Pulsar Key_Shared follow-up

Pulsar also supports `Key_Shared`. With that mode, consumers still share a subscription, but messages with the same key stay on one consumer in order. That is a useful next experiment after Shared.

## Automated Pulsar smoke test

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python smoke_test.py
```

Expected output:

```text
E2E PASS: produced, consumed, retried and dead-lettered successfully
```

## Repository layout

```text
.
├── producer_app.py                  # Pulsar producer UI
├── consumer_app.py                  # Pulsar consumer dashboard
├── eventhub_producer_app.py         # Event Hubs producer UI
├── eventhub_consumer_app.py         # Event Hubs partition ownership dashboard
├── Dockerfile.producer
├── Dockerfile.consumer
├── Dockerfile.eventhub-producer
├── Dockerfile.eventhub-consumer
├── docker-compose.yml               # Local Pulsar visual demo
├── azure/deploy.sh                  # Pulsar-only Azure demo
├── azure/deploy-comparison.sh       # Pulsar vs Event Hubs lab
├── smoke_test.py
└── .github/workflows/e2e.yml
```

## Production note

This is deliberately a learning PoC. The Azure deployment uses connection strings to keep the example easy to understand; production workloads should prefer managed identity/RBAC where practical, private networking, managed secrets, observability, appropriate retention/capacity settings, and production-grade Pulsar topology/storage if Pulsar is selected.
