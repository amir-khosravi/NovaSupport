"""
Kafka topic definitions for NovaSupport.

Single source of truth for topic names, partition counts, and retention.
Import this module wherever a topic name or config is needed.
"""

from dataclasses import dataclass

from config.settings import settings


@dataclass(frozen=True)
class TopicConfig:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int = 7 * 24 * 60 * 60 * 1000  # 7 days


# ──────────────────────────────────────────────────────────────────────────────
# Topic definitions
# ──────────────────────────────────────────────────────────────────────────────

TICKET_CREATED = TopicConfig(
    name=settings.kafka.ticket_topic,
    partitions=3,
    replication_factor=settings.kafka.replication_factor,
)

AGENT_COMMANDS = TopicConfig(
    name=settings.kafka.command_topic,
    partitions=settings.kafka.command_partitions,  # 6 — allows up to 6 parallel workers
    replication_factor=settings.kafka.replication_factor,
)

AGENT_RESULTS = TopicConfig(
    name=settings.kafka.result_topic,
    partitions=settings.kafka.command_partitions,
    replication_factor=settings.kafka.replication_factor,
)

TICKET_COMPLETED = TopicConfig(
    name=settings.kafka.completed_topic,
    partitions=3,
    replication_factor=settings.kafka.replication_factor,
)

TICKET_ESCALATED = TopicConfig(
    name=settings.kafka.escalated_topic,
    partitions=3,
    replication_factor=settings.kafka.replication_factor,
)

AGENT_COMMANDS_RETRY = TopicConfig(
    name=settings.kafka.retry_topic,
    partitions=3,
    replication_factor=settings.kafka.replication_factor,
    retention_ms=24 * 60 * 60 * 1000,  # 1 day — retries expire faster
)

AGENT_COMMANDS_DLQ = TopicConfig(
    name=settings.kafka.dlq_topic,
    partitions=3,
    replication_factor=settings.kafka.replication_factor,
    retention_ms=30 * 24 * 60 * 60 * 1000,  # 30 days — DLQ kept for investigation
)

# Ordered list used by create_topics.py
ALL_TOPICS: list[TopicConfig] = [
    TICKET_CREATED,
    AGENT_COMMANDS,
    AGENT_RESULTS,
    TICKET_COMPLETED,
    TICKET_ESCALATED,
    AGENT_COMMANDS_RETRY,
    AGENT_COMMANDS_DLQ,
]
