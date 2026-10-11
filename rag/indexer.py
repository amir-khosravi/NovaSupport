"""
RAG Indexer — embeds policy chunks and upserts them into Qdrant.

Idempotent: re-running recreates the collection with fresh vectors.

Run: python -m rag.indexer
  or: python scripts/index_policies.py
"""

from __future__ import annotations

import logging
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    PointStruct,
    VectorParams,
)

from config.settings import settings
from rag.chunker import load_all_chunks
from rag.embedder import EMBEDDING_DIM, embed_texts

logger = logging.getLogger(__name__)

COLLECTION_NAME = "novasupport_policies"
KNOWLEDGE_DIR = Path(__file__).resolve().parent.parent / "data" / "knowledge"


def get_client() -> QdrantClient:
    return QdrantClient(url=settings.qdrant.url)


def _recreate_collection(client: QdrantClient) -> None:
    """Drop (if exists) and recreate the collection."""
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION_NAME in existing:
        client.delete_collection(COLLECTION_NAME)
        logger.info("Deleted existing collection: %s", COLLECTION_NAME)

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=EMBEDDING_DIM,
            distance=Distance.COSINE,
        ),
    )
    logger.info(
        "Created collection '%s'  dim=%d  distance=COSINE",
        COLLECTION_NAME, EMBEDDING_DIM,
    )


def index_policies(
    knowledge_dir: Path = KNOWLEDGE_DIR,
    batch_size: int = 32,
    recreate: bool = True,
) -> int:
    """
    Load all markdown files, embed them, and upsert to Qdrant.

    Returns the number of chunks indexed.
    """
    client = get_client()

    if recreate:
        _recreate_collection(client)

    chunks = load_all_chunks(knowledge_dir)
    logger.info("Loaded %d chunks from %d files", len(chunks), len(list(knowledge_dir.glob("*.md"))))

    # Embed in batches
    texts = [c["text"] for c in chunks]
    all_vectors: list[list[float]] = []

    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        vectors = embed_texts(batch)
        all_vectors.extend(vectors)
        logger.info("Embedded chunks %d–%d", start, start + len(batch) - 1)

    # Build Qdrant points
    points = [
        PointStruct(
            id=i,
            vector=vector,
            payload={
                "chunk_id":    chunk["chunk_id"],
                "policy_id":   chunk["policy_id"],
                "title":       chunk["title"],
                "section":     chunk["section"],
                "text":        chunk["text"],
                "source_file": chunk["source_file"],
            },
        )
        for i, (chunk, vector) in enumerate(zip(chunks, all_vectors))
    ]

    # Upsert all at once (demo scale — ~50 chunks is tiny)
    client.upsert(collection_name=COLLECTION_NAME, points=points)
    logger.info("Upserted %d points into '%s'", len(points), COLLECTION_NAME)

    return len(points)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    n = index_policies()
    print(f"\nIndexed {n} policy chunks into Qdrant collection '{COLLECTION_NAME}'")
    sys.exit(0)
