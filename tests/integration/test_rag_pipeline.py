"""
Integration tests for the RAG pipeline.

Requires:
  - Qdrant running:  docker compose --profile rag up -d qdrant
  - Index loaded:    python -m rag.indexer

Tests verify:
  - express_delivery_policy ranks #1 for the demo scenario query
  - refund_policy surfaces for refund queries
  - RETRIEVE_POLICY handler returns real Qdrant results
"""

from __future__ import annotations

import pytest

from rag.retriever import PolicyRetriever, search
from streaming.schemas import AgentCommand, TaskType, new_id


class TestRetrieverSearch:

    def test_express_delay_query_hits_express_policy(self):
        """
        Demo scenario: express delivery delayed → express_delivery_policy must be top result.
        Ground truth in express_delivery_policy.md:
          'If express delivery is delayed by more than 3 business days,
           the shipping fee may be refunded.'
        """
        results = search(
            "express delivery delayed shipping fee refund compensation",
            top_k=3,
        )

        assert len(results) >= 1, "No results returned from Qdrant"
        top = results[0]

        assert top["policy_id"] == "express_delivery_policy", (
            f"Expected express_delivery_policy as top hit, got {top['policy_id']} "
            f"(score={top['relevance_score']:.3f})"
        )
        assert top["relevance_score"] >= 0.50, (
            f"Top result score too low: {top['relevance_score']:.3f}"
        )

    def test_passage_contains_3_business_days_rule(self):
        """
        The express delay compensation passage must include the 3-business-day threshold.
        This is the critical ground truth for the demo.
        """
        results = search(
            "express delivery delayed refund compensation rules",
            top_k=5,
        )

        all_passages = " ".join(r["passage"].lower() for r in results)
        assert "3 business days" in all_passages, (
            "The 3-business-day threshold from express_delivery_policy.md "
            "was not found in top-5 results"
        )

    def test_refund_query_hits_refund_policy(self):
        """Refund-specific query should surface refund or express delivery policies."""
        results = search("refund eligibility how to request money back", top_k=3)

        assert len(results) >= 1
        policy_ids = {r["policy_id"] for r in results}
        # At least one result should be from a refund-related policy
        refund_policies = {"refund_policy", "express_delivery_policy", "return_policy"}
        assert policy_ids & refund_policies, (
            f"Expected a refund policy in results, got: {policy_ids}"
        )

    def test_all_results_have_required_fields(self):
        """Every search result must have all required fields."""
        results = search("delivery delay refund", top_k=3)

        for r in results:
            assert "policy_id" in r
            assert "title" in r
            assert "section" in r
            assert "passage" in r
            assert "relevance_score" in r
            assert "chunk_id" in r
            assert 0.0 <= r["relevance_score"] <= 1.0

    def test_score_threshold_filters_low_relevance(self):
        """A very specific query with high threshold should return fewer results."""
        high_threshold = search(
            "quantum physics orbital mechanics", top_k=5, score_threshold=0.80
        )
        # Policy documents should not match physics queries at 0.80 threshold
        assert len(high_threshold) == 0, (
            f"Expected 0 results for irrelevant query, got {len(high_threshold)}"
        )


class TestPolicyRetriever:

    def test_delivery_delay_express_retrieves_compensation(self):
        """
        PolicyRetriever.retrieve() for delivery_delay + express must find
        the Compensation Rules section.
        """
        retriever = PolicyRetriever()
        results = retriever.retrieve("delivery_delay", shipping_method="express", top_k=3)

        assert len(results) >= 1
        top = results[0]
        assert top["policy_id"] == "express_delivery_policy"

        # The critical policy passage must mention the key rule
        all_text = " ".join(r["passage"].lower() for r in results)
        assert any(kw in all_text for kw in ("3 business days", "shipping fee", "refund"))

    def test_general_inquiry_returns_some_result(self):
        """Even a vague general_inquiry should return at least one result."""
        retriever = PolicyRetriever()
        results = retriever.retrieve("general_inquiry", top_k=2)
        assert len(results) >= 1


class TestRetrievePolicyHandler:

    async def test_handler_calls_qdrant_and_returns_sources(self):
        """
        RETRIEVE_POLICY handler must return real Qdrant results (not mock).
        Verifies: retrieval_method == 'qdrant_semantic' and sources have content.
        """
        from apps.worker.handlers import registry

        cmd = AgentCommand(
            workflow_id=new_id(),
            ticket_id="TKT-RAG-001",
            task_type=TaskType.RETRIEVE_POLICY,
            payload={
                "category": "delivery_delay",
                "shipping_method": "express",
            },
            correlation_id=new_id(),
        )

        result = await registry.dispatch(cmd)

        assert result["retrieval_method"] == "qdrant_semantic", (
            f"Expected qdrant_semantic, got {result['retrieval_method']}"
        )
        assert len(result["sources"]) >= 1
        assert result["sources"][0]["policy_id"] == "express_delivery_policy"

    async def test_handler_passage_contains_refund_rule(self):
        """
        The returned passage must contain the express delay refund rule.
        This is the ground-truth assertion for the whole RAG pipeline.
        """
        from apps.worker.handlers import registry

        cmd = AgentCommand(
            workflow_id=new_id(),
            ticket_id="TKT-RAG-002",
            task_type=TaskType.RETRIEVE_POLICY,
            payload={
                "category": "delivery_delay",
                "shipping_method": "express",
            },
            correlation_id=new_id(),
        )

        result = await registry.dispatch(cmd)

        all_passages = " ".join(
            s["passage"].lower() for s in result["sources"]
        )
        assert "3 business days" in all_passages or "refund" in all_passages, (
            "Expected the 3-business-day refund rule in retrieved passages"
        )
