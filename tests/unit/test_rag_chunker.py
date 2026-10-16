"""Unit tests for the RAG markdown chunker."""

from pathlib import Path
import tempfile

from rag.chunker import chunk_markdown_file, load_all_chunks


SAMPLE_MD = """\
# Express Delivery Policy

**Version:** 1.0

---

## Overview

NovaShop offers Express Delivery with a guaranteed delivery window.

---

## Compensation Rules

If the Express Delivery is delayed by more than 3 business days,
the customer is entitled to a full refund of the shipping fee.

---

## Exceptions

Compensation does not apply when the delay is caused by:
- Incorrect address.
- Force majeure events.
"""


class TestChunker:

    def test_extracts_sections_as_chunks(self, tmp_path):
        path = tmp_path / "test_policy.md"
        path.write_text(SAMPLE_MD, encoding="utf-8")
        chunks = chunk_markdown_file(path)

        section_names = [c["section"] for c in chunks]
        assert "Compensation Rules" in section_names
        assert "Exceptions" in section_names

    def test_chunk_has_required_fields(self, tmp_path):
        path = tmp_path / "express_delivery_policy.md"
        path.write_text(SAMPLE_MD, encoding="utf-8")
        chunks = chunk_markdown_file(path)

        for chunk in chunks:
            assert "chunk_id" in chunk
            assert "policy_id" in chunk
            assert "title" in chunk
            assert "section" in chunk
            assert "text" in chunk
            assert "source_file" in chunk

    def test_policy_id_is_filename_without_extension(self, tmp_path):
        path = tmp_path / "express_delivery_policy.md"
        path.write_text(SAMPLE_MD, encoding="utf-8")
        chunks = chunk_markdown_file(path)

        assert all(c["policy_id"] == "express_delivery_policy" for c in chunks)

    def test_title_extracted_from_h1(self, tmp_path):
        path = tmp_path / "policy_h1.md"
        path.write_text(SAMPLE_MD, encoding="utf-8")
        chunks = chunk_markdown_file(path)

        assert all(c["title"] == "Express Delivery Policy" for c in chunks)

    def test_chunk_text_contains_section_content(self, tmp_path):
        path = tmp_path / "policy_content.md"
        path.write_text(SAMPLE_MD, encoding="utf-8")
        chunks = chunk_markdown_file(path)

        comp = next(c for c in chunks if c["section"] == "Compensation Rules")
        assert "3 business days" in comp["text"]
        assert "refund" in comp["text"].lower()

    def test_load_all_chunks_counts_files(self, tmp_path):
        for name in ["policy_a.md", "policy_b.md"]:
            (tmp_path / name).write_text(SAMPLE_MD, encoding="utf-8")

        chunks = load_all_chunks(tmp_path)
        policy_ids = {c["policy_id"] for c in chunks}
        assert "policy_a" in policy_ids
        assert "policy_b" in policy_ids
