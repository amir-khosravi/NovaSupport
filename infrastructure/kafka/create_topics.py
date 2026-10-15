"""
Create all Kafka topics required by NovaSupport.

Idempotent — safe to run multiple times.
Skips topics that already exist.

Run: python infrastructure/kafka/create_topics.py
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # noqa: E402
from aiokafka.errors import TopicAlreadyExistsError  # noqa: E402

from config.settings import settings  # noqa: E402
from streaming.topics import ALL_TOPICS  # noqa: E402


async def create_all_topics() -> None:
    print(f"Connecting to Kafka at {settings.kafka.bootstrap_servers} ...")

    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        request_timeout_ms=15_000,
    )
    await admin.start()

    try:
        new_topics = [
            NewTopic(
                name=t.name,
                num_partitions=t.partitions,
                replication_factor=t.replication_factor,
                topic_configs={"retention.ms": str(t.retention_ms)},
            )
            for t in ALL_TOPICS
        ]

        print(f"\nCreating {len(new_topics)} topics:\n")
        print(f"  {'Topic':<35} {'Partitions':>10} {'Replication':>12}")
        print("  " + "-" * 60)
        for t in ALL_TOPICS:
            print(f"  {t.name:<35} {t.partitions:>10} {t.replication_factor:>12}")

        results = await admin.create_topics(new_topics, validate_only=False)

        print("\nResults:")
        all_ok = True
        for entry in results.topic_errors:
            # aiokafka returns (topic, error_code) or (topic, error_code, error_message)
            topic = entry[0]
            error = entry[1]
            if error == 0:
                print(f"  [OK]      {topic}")
            elif error == 36:  # TopicAlreadyExistsError code
                print(f"  [EXISTS]  {topic}")
            else:
                print(f"  [ERROR]   {topic}  (code={error})")
                all_ok = False

        if all_ok:
            print("\nAll topics ready.")
        else:
            print("\nSome topics failed. Check Kafka logs.")
            sys.exit(1)

    finally:
        await admin.close()


async def list_topics() -> None:
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        request_timeout_ms=10_000,
    )
    await admin.start()
    try:
        topics = await admin.list_topics()
        novasupport = sorted(t for t in topics if not t.startswith("__"))
        print(f"\nTopics on broker ({len(novasupport)} NovaSupport topics):")
        for t in novasupport:
            print(f"  {t}")
    finally:
        await admin.close()


async def main() -> None:
    print("NovaSupport — Kafka Topic Setup\n")
    await create_all_topics()
    await list_topics()


if __name__ == "__main__":
    asyncio.run(main())
