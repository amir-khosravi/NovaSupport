"""Quick test of the RAG retriever for the demo scenario."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rag.retriever import PolicyRetriever

r = PolicyRetriever()

print("=== Demo query: express delivery delayed refund ===\n")
results = r.retrieve("delivery_delay", shipping_method="express", top_k=3)
for i, h in enumerate(results, 1):
    score = h["relevance_score"]
    pid = h["policy_id"]
    section = h["section"]
    passage = h["passage"][:150]
    print(f"{i}. [{score:.3f}] {pid} / {section}")
    print(f"   {passage}...\n")

print("=== General inquiry ===\n")
results2 = r.retrieve("general_inquiry", top_k=2)
for i, h in enumerate(results2, 1):
    print(f"{i}. [{h['relevance_score']:.3f}] {h['policy_id']} / {h['section']}")
