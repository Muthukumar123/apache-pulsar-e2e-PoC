import argparse
import json
import os
import time
import uuid

import pulsar

SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
TOPIC = os.getenv("PULSAR_TOPIC", "persistent://public/default/poc-events")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish demo events to Apache Pulsar")
    parser.add_argument("--count", type=int, default=5, help="Number of normal events to publish")
    parser.add_argument(
        "--include-poison",
        action="store_true",
        help="Publish one poison event to demonstrate retry/DLQ behavior",
    )
    return parser.parse_args()


def build_event(sequence: int, event_type: str = "demo") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "type": event_type,
        "sequence": sequence,
        "created_at_epoch_ms": int(time.time() * 1000),
        "payload": {"message": f"Apache Pulsar PoC event {sequence}"},
    }


def main() -> None:
    args = parse_args()
    client = pulsar.Client(SERVICE_URL)
    producer = client.create_producer(TOPIC)

    try:
        for sequence in range(1, args.count + 1):
            event = build_event(sequence)
            message_id = producer.send(json.dumps(event).encode("utf-8"))
            print(f"published event_id={event['event_id']} message_id={message_id}")

        if args.include_poison:
            poison = build_event(args.count + 1, event_type="poison")
            message_id = producer.send(json.dumps(poison).encode("utf-8"))
            print(f"published POISON event_id={poison['event_id']} message_id={message_id}")
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
