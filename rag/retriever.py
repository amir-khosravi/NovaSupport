"""
RAG Retriever — semantic search over the Qdrant policy collection.

Uses direct HTTP calls to /points/search to avoid qdrant-client v1.18
redirecting search() to the newer /points/query endpoint (unsupported
on Qdrant server v1.9.4).

Usage:
    from rag.retriever import PolicyRetriever

    retriever = PolicyRetriever()
    results = retriever.search("express delivery delayed refund", top_k=3)
    # [{"policy_id": "express_delivery_policy", "title": "...", "passage": "...", "relevance_score": 0.94}]
"""

from __future__ import annotations

import logging
from functools import lru_cache

import httpx

from config.settings import settings
from rag.embedder import embed_query
from rag.indexer import COLLECTION_NAME

logger = logging.getLogger(__name__)

MAX_PASSAGE_CHARS = 600


@lru_cache(maxsize=1)
def _qdrant_url() -> str:
    return settings.qdrant.url.rstrip("/")


def _extract_passage(text: str, max_chars: int = MAX_PASSAGE_CHARS) -> str:
    import re
    text = re.sub(r"\|[-:]+\|[-:\s|]+", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = text.strip().splitlines()
    if lines and lines[0].startswith("#"):
        lines = lines[1:]
    text = "\n".join(lines).strip()

    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_period = truncated.rfind(". ")
    if last_period > max_chars // 2:
        return truncated[: last_period + 1]
    return truncated + "..."


def search(
    query: str,
    top_k: int = 3,
    score_threshold: float = 0.30,
    filter_policy_ids: list[str] | None = None,
) -> list[dict]:
    """
    Semantic search over policy chunks via direct HTTP to /points/search.

    Returns:
        List of dicts: [{policy_id, title, section, passage, relevance_score, chunk_id}]
    """
    query_vector = embed_query(query)

    payload: dict = {
        "vector": query_vector,
        "limit": top_k,
        "score_threshold": score_threshold,
        "with_payload": True,
    }
    if filter_policy_ids:
        payload["filter"] = {
            "must": [
                {"key": "policy_id", "match": {"any": filter_policy_ids}}
            ]
        }

    url = f"{_qdrant_url()}/collections/{COLLECTION_NAME}/points/search"
    response = httpx.post(url, json=payload, timeout=10.0)
    response.raise_for_status()

    hits = response.json().get("result", [])

    results = []
    for hit in hits:
        p = hit.get("payload") or {}
        results.append({
            "policy_id":       p.get("policy_id", "unknown"),
            "title":           p.get("title", ""),
            "section":         p.get("section", ""),
            "passage":         _extract_passage(p.get("text", "")),
            "relevance_score": round(float(hit.get("score", 0.0)), 4),
            "chunk_id":        p.get("chunk_id", ""),
        })

    logger.debug(
        "RAG search query=%r  hits=%d  top_score=%.3f",
        query[:60], len(results),
        results[0]["relevance_score"] if results else 0.0,
    )
    return results


class PolicyRetriever:
    """Wrapper around search() that builds a query from structured ticket inputs."""

    def retrieve(
        self,
        category: str,
        shipping_method: str = "standard",
        top_k: int = 3,
    ) -> list[dict]:
        category_queries = {
            "delivery_delay": (
                "delivery delayed shipping fee refund compensation express"
                if shipping_method == "express"
                else "delivery delayed late shipment customer rights"
            ),
            "refund_request":    "refund policy eligibility how to request money back",
            "request_refund":    "refund policy eligibility how to request money back",
            "wrong_item":        "wrong item received incorrect product return exchange",
            "billing_issue":     "payment problem billing duplicate charge investigation",
            "duplicate_charge":  "duplicate charge double billing payment refund",
            "damaged_product":   "damaged product arrived broken return replacement",
            "cancel_order":      "order cancellation policy how to cancel refund",
            "account_issue":     "account login security password reset",
            "general_inquiry":   "customer support policy help contact",
        }

        query = category_queries.get(
            category,
            f"{category.replace('_', ' ')} customer support policy",
        )

        logger.info(
            "RAG retrieve  category=%s  shipping=%s  query=%r",
            category, shipping_method, query,
        )
        return search(query, top_k=top_k)
