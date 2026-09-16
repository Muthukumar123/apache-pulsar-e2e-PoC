import json
import os
import time
import uuid
import urllib.error
import urllib.request

import pulsar

SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
ADMIN_URL = os.getenv("PULSAR_ADMIN_URL", "http://localhost:8080")
GOOD_EVENT_COUNT = int(os.getenv("E2E_GOOD_EVENT_COUNT", "3"))
MAX_REDELIVERIES = 3
NEGATIVE_ACK_DELAY_MS = 500
TIMEOUT_SECONDS = int(os.getenv("E2E_TIMEOUT_SECONDS", "45"))


def event(event_type: str, sequence: int, run_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "run_id": run_id,
        "type": event_type,
        "sequence": sequence,
        "payload": {"message": f"e2e-{event_type}-{sequence}"},
    }


def wait_for_default_namespace() -> None:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    url = f"{ADMIN_URL}/admin/v2/namespaces/public"

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                namespaces = json.loads(response.read().decode("utf-8"))
            if "public/default" in namespaces:
                print("Pulsar namespace public/default is ready")
                return
        except (urllib.error.URLError, json.JSONDecodeError):
            pass
        time.sleep(1)

    raise RuntimeError("Pulsar namespace public/default did not become ready")


def create_topic(topic_name: str) -> None:
    local_name = topic_name.rsplit("/", 1)[-1]
    url = f"{ADMIN_URL}/admin/v2/persistent/public/default/{local_name}"
    request = urllib.request.Request(
        url,
        method="PUT",
        data=b"{}",
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            response.read()
        print(f"Created topic {topic_name}")
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            print(f"Topic already exists {topic_name}")
            return
        raise


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    topic = f"persistent://public/default/poc-e2e-{run_id}"
    dlq_topic = f"persistent://public/default/poc-e2e-{run_id}-dlq"
    subscription = f"poc-e2e-workers-{run_id}"

    print(f"run_id={run_id}")
    print(f"topic={topic}")
    print(f"dlq_topic={dlq_topic}")

    wait_for_default_namespace()
    create_topic(topic)
    create_topic(dlq_topic)

    client = pulsar.Client(SERVICE_URL)
    main_consumer = None
    dlq_consumer = None
    producer = None

    try:
        dlq_consumer = client.subscribe(
            dlq_topic,
            subscription_name=f"poc-e2e-dlq-reader-{run_id}",
            initial_position=pulsar.InitialPosition.Earliest,
        )

        main_consumer = client.subscribe(
            topic,
            subscription_name=subscription,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            negative_ack_redelivery_delay_ms=NEGATIVE_ACK_DELAY_MS,
            dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(
                max_redeliver_count=MAX_REDELIVERIES,
                dead_letter_topic=dlq_topic,
                initial_subscription_name=f"{subscription}-dlq-initial",
            ),
        )

        producer = client.create_producer(topic)

        expected_good_ids = set()
        for sequence in range(1, GOOD_EVENT_COUNT + 1):
            payload = event("demo", sequence, run_id)
            expected_good_ids.add(payload["event_id"])
            producer.send(json.dumps(payload).encode("utf-8"))
            print(f"PRODUCED good event_id={payload['event_id']}")

        poison = event("poison", GOOD_EVENT_COUNT + 1, run_id)
        producer.send(json.dumps(poison).encode("utf-8"))
        print(f"PRODUCED poison event_id={poison['event_id']}")

        acknowledged_ids = set()
        poison_deliveries = 0
        dlq_event = None
        deadline = time.monotonic() + TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            try:
                message = main_consumer.receive(timeout_millis=500)
            except pulsar.Timeout:
                message = None

            if message is not None:
                payload = json.loads(message.data().decode("utf-8"))
                if payload.get("run_id") != run_id:
                    main_consumer.acknowledge(message)
                elif payload.get("type") == "poison":
                    poison_deliveries += 1
                    print(
                        "RETRY poison "
                        f"delivery={poison_deliveries} redelivery_count={message.redelivery_count()}"
                    )
                    main_consumer.negative_acknowledge(message)
                else:
                    acknowledged_ids.add(payload["event_id"])
                    main_consumer.acknowledge(message)
                    print(f"ACK event_id={payload['event_id']}")

            try:
                dead_letter_message = dlq_consumer.receive(timeout_millis=150)
            except pulsar.Timeout:
                dead_letter_message = None

            if dead_letter_message is not None:
                candidate = json.loads(dead_letter_message.data().decode("utf-8"))
                dlq_consumer.acknowledge(dead_letter_message)
                if candidate.get("run_id") == run_id:
                    dlq_event = candidate
                    print(f"DLQ event_id={candidate.get('event_id')}")

            if acknowledged_ids == expected_good_ids and dlq_event is not None:
                break

        assert acknowledged_ids == expected_good_ids, (
            f"not all normal events were acknowledged: expected={expected_good_ids}, "
            f"actual={acknowledged_ids}"
        )
        assert poison_deliveries >= 2, "poison event was not redelivered"
        assert dlq_event is not None, "poison event never arrived on the DLQ"
        assert dlq_event["event_id"] == poison["event_id"], "unexpected event reached DLQ"
        assert dlq_event["type"] == "poison", "DLQ event was not the poison event"

        print(
            "E2E PASS: produced, consumed, retried and dead-lettered successfully "
            f"(poison deliveries={poison_deliveries})"
        )
    finally:
        if producer is not None:
            producer.close()
        if main_consumer is not None:
            main_consumer.close()
        if dlq_consumer is not None:
            dlq_consumer.close()
        client.close()


if __name__ == "__main__":
    main()
