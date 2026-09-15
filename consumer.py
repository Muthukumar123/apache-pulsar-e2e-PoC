import json
import os
import signal

import pulsar

SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
TOPIC = os.getenv("PULSAR_TOPIC", "persistent://public/default/poc-events")
SUBSCRIPTION = os.getenv("PULSAR_SUBSCRIPTION", "poc-workers")
DLQ_TOPIC = os.getenv("PULSAR_DLQ_TOPIC", "persistent://public/default/poc-events-dlq")
MAX_REDELIVERIES = int(os.getenv("PULSAR_MAX_REDELIVERIES", "3"))
NEGATIVE_ACK_DELAY_MS = int(os.getenv("PULSAR_NEGATIVE_ACK_DELAY_MS", "1000"))

running = True


def stop(_signum, _frame) -> None:
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    client = pulsar.Client(SERVICE_URL)
    consumer = client.subscribe(
        TOPIC,
        subscription_name=SUBSCRIPTION,
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest,
        negative_ack_redelivery_delay_ms=NEGATIVE_ACK_DELAY_MS,
        dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(
            max_redeliver_count=MAX_REDELIVERIES,
            dead_letter_topic=DLQ_TOPIC,
            initial_subscription_name=f"{SUBSCRIPTION}-dlq",
        ),
    )

    print(
        f"consumer started topic={TOPIC} subscription={SUBSCRIPTION} "
        f"dlq={DLQ_TOPIC} max_redeliveries={MAX_REDELIVERIES}"
    )

    try:
        while running:
            try:
                message = consumer.receive(timeout_millis=1000)
            except pulsar.Timeout:
                continue

            try:
                event = json.loads(message.data().decode("utf-8"))
                redelivery_count = message.redelivery_count()
                print(
                    f"received event_id={event.get('event_id')} type={event.get('type')} "
                    f"redelivery_count={redelivery_count}"
                )

                if event.get("type") == "poison":
                    print("processing failed intentionally; negative acknowledging")
                    consumer.negative_acknowledge(message)
                    continue

                consumer.acknowledge(message)
                print(f"acknowledged event_id={event.get('event_id')}")
            except Exception as exc:
                print(f"processing error={exc!r}; negative acknowledging")
                consumer.negative_acknowledge(message)
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
