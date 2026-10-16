"""
Integration test: full Kafka roundtrip.

Sends a real event to Redpanda and reads it back.
Requires Redpanda running: docker compose up -d redpanda

Run: pytest tests/integration/test_kafka_roundtrip.py -v
"""

import asyncio

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.settings import settings
from streaming.producer import KafkaEventProducer
from streaming.schemas import (
    AgentCommand,
    AgentResult,
    TaskStatus,
    TaskType,
    TicketCreatedEvent,
    deserialize_command,
    deserialize_result,
    deserialize_ticket_created,
    new_id,
    serialize,
)

BOOTSTRAP = settings.kafka.bootstrap_servers

# Use a dedicated test topic so we don't pollute main topics
TEST_TOPIC = "test.roundtrip"
TEST_GROUP = "test-consumer-group"


async def _ensure_topic(name: str) -> None:
    """Create test topic if it doesn't exist (Redpanda allows auto-create via admin)."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)])
    except Exception:
        pass  # already exists
    finally:
        await admin.close()


async def _send_and_receive(event_bytes: bytes) -> bytes:
    """
    Send bytes to TEST_TOPIC and read them back.

    Order matters:
      1. Start consumer at 'latest' — positioned AFTER existing messages
      2. Wait for partition assignment
      3. Send the new message
      4. Consumer receives only the new message
    """
    group = f"{TEST_GROUP}-{new_id()}"

    # Step 1: start consumer first, positioned at latest offset
    consumer = AIOKafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=6_000,
    )
    await consumer.start()

    # Step 2: wait for partition assignment before sending
    await asyncio.sleep(0.5)

    # Step 3: send the message
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await producer.start()
    await producer.send_and_wait(TEST_TOPIC, value=event_bytes)
    await producer.stop()

    # Step 4: receive
    received = None
    try:
        async for record in consumer:
            received = record.value
            break
    finally:
        await consumer.stop()

    assert received is not None, "No message received from Kafka within timeout"
    return received


@pytest.fixture(scope="module", autouse=True)
async def setup_test_topic():
    await _ensure_topic(TEST_TOPIC)
    yield


# ──────────────────────────────────────────────────────────────────────────────

class TestKafkaRoundtrip:

    async def test_ticket_created_roundtrip(self):
        """TicketCreatedEvent survives serialize → Kafka → deserialize."""
        event = TicketCreatedEvent(
            ticket_id="TKT-0001",
            customer_id="C-0001",
            order_id="ORD-0001",
            message="My express delivery is 5 days late. Can I get a refund?",
        )
        raw = serialize(event)
        received = await _send_and_receive(raw)
        restored = deserialize_ticket_created(received)

        assert restored.ticket_id == "TKT-0001"
        assert restored.order_id == "ORD-0001"
        assert restored.workflow_id == event.workflow_id
        assert restored.event_id == event.event_id
        assert "refund" in restored.message

    async def test_agent_command_roundtrip(self):
        """AgentCommand with payload survives Kafka roundtrip."""
        corr_id = new_id()
        cmd = AgentCommand(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_type=TaskType.TRIAGE_TICKET,
            payload={"message": "My order is late.", "language": "en"},
            correlation_id=corr_id,
        )
        raw = serialize(cmd)
        received = await _send_and_receive(raw)
        restored = deserialize_command(received)

        assert restored.task_type == "triage_ticket"
        assert restored.correlation_id == corr_id
        assert restored.payload["language"] == "en"

    async def test_agent_result_roundtrip(self):
        """AgentResult with nested result dict survives Kafka roundtrip."""
        result = AgentResult(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_id="TASK-001",
            task_type=TaskType.TRIAGE_TICKET,
            status=TaskStatus.COMPLETED,
            result={
                "category": "delivery_delay",
                "priority": "high",
                "requires_order_lookup": True,
                "requires_policy_retrieval": True,
            },
            correlation_id="CORR-001",
        )
        raw = serialize(result)
        received = await _send_and_receive(raw)
        restored = deserialize_result(received)

        assert restored.is_success()
        assert restored.result["category"] == "delivery_delay"
        assert restored.result["requires_policy_retrieval"] is True

    async def test_producer_context_manager(self):
        """KafkaEventProducer works as async context manager."""
        event = TicketCreatedEvent(
            ticket_id="TKT-TEST",
            customer_id="C-TEST",
            message="Testing producer context manager",
        )
        async with KafkaEventProducer() as producer:
            # send to the real ticket.created topic (won't affect other tests)
            await producer.send(
                settings.kafka.ticket_topic,
                event,
                key=event.workflow_id,
            )
        # No exception = producer flushed and closed cleanly

    async def test_partition_key_routing(self):
        """Same workflow_id key routes to same partition on repeated sends."""
        wf_id = new_id()
        partitions_seen = set()

        producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await producer.start()
        try:
            for _ in range(3):
                meta = await producer.send_and_wait(
                    settings.kafka.command_topic,
                    value=b"{}",
                    key=wf_id,
                )
                partitions_seen.add(meta.partition)
        finally:
            await producer.stop()

        # All messages with same key must land on same partition
        assert len(partitions_seen) == 1, (
            f"Expected 1 partition, got {partitions_seen}"
        )
