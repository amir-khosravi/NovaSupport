"""
Unit tests for Phase 10 LLM agent handlers.

All Claude API calls are mocked — no network required.
Tests verify:
  - LLM output is correctly mapped to the handler contract
  - Mock fallback activates when LLM raises (no API key, timeout, etc.)
  - Field constraints are preserved (category, priority, action values)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agents.triage import TriageResult
from agents.resolver import ResolverResult
from apps.worker.handlers import registry as default_registry
from streaming.schemas import AgentCommand, TaskType, new_id


def _cmd(task_type: TaskType, payload: dict | None = None) -> AgentCommand:
    return AgentCommand(
        workflow_id=new_id(),
        ticket_id="TKT-LLM-001",
        task_type=task_type,
        payload=payload or {},
        correlation_id=new_id(),
    )


# ── TRIAGE_TICKET ─────────────────────────────────────────────────────────────

class TestTriageHandler:

    async def test_llm_result_mapped_correctly(self):
        llm_output = TriageResult(
            category="delivery_delay",
            priority="high",
            requires_order_lookup=True,
            requires_policy_retrieval=True,
            language="en",
        )
        with patch("agents.triage.triage_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.return_value = llm_output

            result = await default_registry.dispatch(
                _cmd(TaskType.TRIAGE_TICKET, {"message": "My express order is 5 days late"})
            )

        assert result["category"] == "delivery_delay"
        assert result["priority"] == "high"
        assert result["requires_order_lookup"] is True
        assert result["requires_policy_retrieval"] is True
        assert result["language"] == "en"
        mock_fn.assert_called_once()

    async def test_general_inquiry_low_priority(self):
        llm_output = TriageResult(
            category="general_inquiry",
            priority="low",
            requires_order_lookup=False,
            requires_policy_retrieval=False,
            language="en",
        )
        with patch("agents.triage.triage_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.return_value = llm_output

            result = await default_registry.dispatch(
                _cmd(TaskType.TRIAGE_TICKET, {"message": "What are your store hours?"})
            )

        assert result["category"] == "general_inquiry"
        assert result["priority"] == "low"
        assert result["requires_order_lookup"] is False

    async def test_keyword_fallback_on_llm_error(self):
        with patch("agents.triage.triage_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.side_effect = ValueError("ANTHROPIC_API_KEY not configured")

            result = await default_registry.dispatch(
                _cmd(TaskType.TRIAGE_TICKET, {
                    "message": "My express delivery is 5 days late, please help!",
                    "language": "en",
                })
            )

        # keyword fallback should still classify correctly
        assert result["category"] == "delivery_delay"
        assert result["priority"] == "high"
        assert result["requires_order_lookup"] is True

    async def test_keyword_fallback_refund_request(self):
        with patch("agents.triage.triage_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.side_effect = RuntimeError("API unavailable")

            result = await default_registry.dispatch(
                _cmd(TaskType.TRIAGE_TICKET, {"message": "I want a refund for order #123"})
            )

        assert result["category"] == "refund_request"

    async def test_keyword_fallback_general_inquiry(self):
        with patch("agents.triage.triage_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.side_effect = RuntimeError("API unavailable")

            result = await default_registry.dispatch(
                _cmd(TaskType.TRIAGE_TICKET, {"message": "How do I update my account email?"})
            )

        assert result["category"] == "general_inquiry"
        assert result["priority"] == "low"


# ── RESOLVE_TICKET ────────────────────────────────────────────────────────────

class TestResolveHandler:

    async def test_llm_refund_decision_mapped_correctly(self):
        llm_output = ResolverResult(
            recommended_action="refund_shipping_fee",
            refund_amount=24.99,
            resolution_notes="Express delay exceeds 3-day threshold per policy.",
            confidence=0.92,
        )
        with patch("agents.resolver.resolve_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.return_value = llm_output

            result = await default_registry.dispatch(
                _cmd(TaskType.RESOLVE_TICKET, {
                    "category": "delivery_delay",
                    "days_delayed": 5,
                    "shipping_fee": 24.99,
                    "policy_sources": [],
                    "customer_tier": "gold",
                })
            )

        assert result["recommended_action"] == "refund_shipping_fee"
        assert result["refund_amount"] == pytest.approx(24.99)
        assert result["confidence"] == pytest.approx(0.92)
        assert "resolution_notes" in result

    async def test_llm_follow_up_decision(self):
        llm_output = ResolverResult(
            recommended_action="follow_up",
            refund_amount=0.0,
            resolution_notes="No automatic resolution available.",
            confidence=0.70,
        )
        with patch("agents.resolver.resolve_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.return_value = llm_output

            result = await default_registry.dispatch(
                _cmd(TaskType.RESOLVE_TICKET, {
                    "category": "general_inquiry",
                    "days_delayed": 0,
                    "shipping_fee": 0.0,
                    "policy_sources": [],
                })
            )

        assert result["recommended_action"] == "follow_up"
        assert result["refund_amount"] == pytest.approx(0.0)

    async def test_rule_fallback_express_delay(self):
        with patch("agents.resolver.resolve_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.side_effect = ValueError("ANTHROPIC_API_KEY not configured")

            result = await default_registry.dispatch(
                _cmd(TaskType.RESOLVE_TICKET, {
                    "category": "delivery_delay",
                    "days_delayed": 5,
                    "shipping_fee": 24.99,
                    "policy_sources": [],
                })
            )

        assert result["recommended_action"] == "refund_shipping_fee"
        assert result["refund_amount"] == pytest.approx(24.99)

    async def test_rule_fallback_on_time_no_refund(self):
        with patch("agents.resolver.resolve_ticket", new_callable=AsyncMock) as mock_fn:
            mock_fn.side_effect = RuntimeError("API unavailable")

            result = await default_registry.dispatch(
                _cmd(TaskType.RESOLVE_TICKET, {
                    "category": "delivery_delay",
                    "days_delayed": 1,
                    "shipping_fee": 9.99,
                    "policy_sources": [],
                })
            )

        assert result["recommended_action"] == "follow_up"
        assert result["refund_amount"] == pytest.approx(0.0)


# ── GENERATE_RESPONSE ─────────────────────────────────────────────────────────

class TestGenerateResponseHandler:

    async def test_llm_response_text_returned(self):
        llm_text = (
            "Dear Alex, we sincerely apologise for the delay with ORD-0001. "
            "Your $24.99 CAD shipping refund has been initiated — "
            "please allow 5 business days for processing."
        )
        with patch("agents.responder.generate_response", new_callable=AsyncMock) as mock_fn:
            mock_fn.return_value = llm_text

            result = await default_registry.dispatch(
                _cmd(TaskType.GENERATE_RESPONSE, {
                    "order_id": "ORD-0001",
                    "recommended_action": "refund_shipping_fee",
                    "days_delayed": 5,
                    "shipping_fee": 24.99,
                    "refund_amount": 24.99,
                    "customer_name": "Alex",
                    "resolution_notes": "Delay exceeds threshold.",
                })
            )

        assert result["response_text"] == llm_text
        assert "refund" in result["response_text"].lower()
        assert result["recommended_action"] == "refund_shipping_fee"

    async def test_template_fallback_on_llm_error(self):
        with patch("agents.responder.generate_response", new_callable=AsyncMock) as mock_fn:
            mock_fn.side_effect = ValueError("ANTHROPIC_API_KEY not configured")

            result = await default_registry.dispatch(
                _cmd(TaskType.GENERATE_RESPONSE, {
                    "order_id": "ORD-0001",
                    "recommended_action": "refund_shipping_fee",
                    "days_delayed": 5,
                    "shipping_fee": 24.99,
                    "refund_amount": 24.99,
                    "customer_name": "Alex",
                })
            )

        assert "refund" in result["response_text"].lower()
        assert "ORD-0001" in result["response_text"]

    async def test_template_fallback_follow_up(self):
        with patch("agents.responder.generate_response", new_callable=AsyncMock) as mock_fn:
            mock_fn.side_effect = RuntimeError("API down")

            result = await default_registry.dispatch(
                _cmd(TaskType.GENERATE_RESPONSE, {
                    "order_id": "ORD-0002",
                    "recommended_action": "follow_up",
                    "days_delayed": 1,
                    "refund_amount": 0.0,
                })
            )

        assert "ORD-0002" in result["response_text"]
        assert result["recommended_action"] == "follow_up"
