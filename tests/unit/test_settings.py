"""Tests for application settings loading."""

import pytest
from config.settings import Settings


def test_settings_load_with_defaults():
    """Settings must load without errors using default values."""
    s = Settings()
    assert s.app.name == "novasupport"
    assert s.app.env == "development"


def test_kafka_defaults():
    s = Settings()
    assert s.kafka.command_topic == "agent.commands"
    assert s.kafka.result_topic == "agent.results"
    assert s.kafka.consumer_group == "support-agent-workers"
    assert s.kafka.command_partitions == 6


def test_rag_defaults():
    s = Settings()
    assert s.rag.chunk_size == 512
    assert s.rag.top_k == 5


def test_worker_defaults():
    s = Settings()
    assert s.worker.max_retries == 3
    assert s.worker.task_timeout_seconds == 60


def test_settings_override_via_env(monkeypatch):
    """Environment variables must override defaults."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("KAFKA_COMMAND_PARTITIONS", "12")
    s = Settings()
    assert s.app.log_level == "DEBUG"
    assert s.kafka.command_partitions == 12


def test_singleton_import():
    """The shared settings instance must be importable."""
    from config.settings import settings
    assert settings.app.name == "novasupport"
