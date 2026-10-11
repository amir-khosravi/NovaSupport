"""
Markdown policy file chunker.

Strategy: split each file by ## headings (H2 sections).
Each chunk = one section with its heading as context prefix.

This preserves semantic coherence: a section about "Shipping Fee Refund"
stays together as one retrievable unit rather than being split mid-sentence.

Output per chunk:
  {
    "chunk_id"   : "express_delivery_policy__compensation_rules",
    "policy_id"  : "express_delivery_policy",
    "title"      : "Express Delivery Policy",
    "section"    : "Compensation Rules",
    "text"       : "## Compensation Rules\n\nIf the Express...",
    "source_file": "express_delivery_policy.md",
  }
"""

from __future__ import annotations

import re
from pathlib import Path


def _slug(text: str) -> str:
    """Convert heading text to a filesystem-safe slug."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _extract_h1_title(content: str) -> str:
    """Extract the first H1 heading as the document title."""
    for line in content.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return "Unknown Policy"


def chunk_markdown_file(path: Path) -> list[dict]:
    """
    Split a markdown file into section-level chunks.

    Returns one dict per H2 section (## heading).
    The document intro (text before first H2) is kept as the first chunk
    if it contains meaningful content (>50 chars).
    """
    content = path.read_text(encoding="utf-8")
    policy_id = path.stem  # filename without extension
    title = _extract_h1_title(content)
    chunks: list[dict] = []

    # Split on H2 headings
    # Pattern: line starting with "## " optionally after newline
    parts = re.split(r"(?m)^(## .+)$", content)

    # parts alternates: [intro_text, heading, body, heading, body, ...]
    # First element is everything before the first H2
    intro = parts[0].strip()
    if len(intro) > 50:
        chunks.append({
            "chunk_id": f"{policy_id}__overview",
            "policy_id": policy_id,
            "title": title,
            "section": "Overview",
            "text": intro,
            "source_file": path.name,
        })

    # Process heading + body pairs
    i = 1
    while i < len(parts) - 1:
        heading_line = parts[i].strip()          # e.g. "## Compensation Rules"
        body = parts[i + 1].strip() if i + 1 < len(parts) else ""
        section_name = heading_line.lstrip("#").strip()
        chunk_text = f"{heading_line}\n\n{body}"

        if len(chunk_text.strip()) > 30:  # skip empty/trivial sections
            chunks.append({
                "chunk_id": f"{policy_id}__{_slug(section_name)}",
                "policy_id": policy_id,
                "title": title,
                "section": section_name,
                "text": chunk_text,
                "source_file": path.name,
            })
        i += 2

    return chunks


def load_all_chunks(knowledge_dir: Path) -> list[dict]:
    """Load and chunk all .md files in a directory."""
    chunks: list[dict] = []
    for md_file in sorted(knowledge_dir.glob("*.md")):
        file_chunks = chunk_markdown_file(md_file)
        chunks.extend(file_chunks)
    return chunks
