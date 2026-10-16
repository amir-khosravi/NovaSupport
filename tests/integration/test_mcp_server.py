"""
Integration tests for the MCP server tool functions.

Calls the query_* functions directly against a real PostgreSQL database.
The functions are thin asyncpg wrappers; we skip the MCP protocol layer.

Requires:
  docker compose --profile db up -d postgres
  python scripts/seed_database.py
"""

from __future__ import annotations

import pytest
import asyncpg

from mcp_servers.operational_data.server import (
    query_customer,
    query_order,
    query_payment,
    query_shipment,
)


def _asyncpg_dsn() -> str:
    from config.settings import settings
    return settings.postgres.url.replace("postgresql+asyncpg://", "postgresql://")


@pytest.fixture
async def pool():
    p = await asyncpg.create_pool(_asyncpg_dsn(), min_size=1, max_size=3)
    yield p
    await p.close()


# ── customer ──────────────────────────────────────────────────────────────────

class TestQueryCustomer:

    async def test_returns_alice_smith(self, pool):
        result = await query_customer(pool, "C-0001")
        assert result["customer_id"] == "C-0001"
        assert result["full_name"] == "Alice Smith"
        assert result["email"] == "alice.smith@example.com"
        assert result["data_source"] == "postgresql"

    async def test_vip_account_status(self, pool):
        result = await query_customer(pool, "C-0001")
        assert result["account_status"] == "vip"

    async def test_not_found_returns_error_dict(self, pool):
        result = await query_customer(pool, "C-9999")
        assert "error" in result
        assert result["customer_id"] == "C-9999"


# ── order ─────────────────────────────────────────────────────────────────────

class TestQueryOrder:

    async def test_returns_ord0001_express(self, pool):
        result = await query_order(pool, "ORD-0001")
        assert result["order_id"] == "ORD-0001"
        assert result["shipping_method"] == "express"
        assert result["data_source"] == "postgresql"

    async def test_includes_shipping_fee_from_payments(self, pool):
        result = await query_order(pool, "ORD-0001")
        assert float(result["shipping_fee"]) == pytest.approx(24.99)

    async def test_includes_items(self, pool):
        result = await query_order(pool, "ORD-0001")
        assert isinstance(result["items"], list)
        assert len(result["items"]) >= 1
        assert "product_name" in result["items"][0]

    async def test_not_found_returns_error_dict(self, pool):
        result = await query_order(pool, "ORD-9999")
        assert "error" in result
        assert result["order_id"] == "ORD-9999"


# ── payment ───────────────────────────────────────────────────────────────────

class TestQueryPayment:

    async def test_returns_completed_payment(self, pool):
        result = await query_payment(pool, "ORD-0001")
        assert result["order_id"] == "ORD-0001"
        assert result["payment_status"] == "completed"
        assert result["data_source"] == "postgresql"

    async def test_shipping_fee_matches_order(self, pool):
        result = await query_payment(pool, "ORD-0001")
        assert float(result["shipping_fee"]) == pytest.approx(24.99)

    async def test_duplicate_charge_flag(self, pool):
        result = await query_payment(pool, "ORD-0001")
        # PAY-0001 has duplicate_charge=True in CSV
        assert result["duplicate_charge"] is True

    async def test_not_found_returns_error_dict(self, pool):
        result = await query_payment(pool, "ORD-9999")
        assert "error" in result


# ── shipment ──────────────────────────────────────────────────────────────────

class TestQueryShipment:

    async def test_returns_delayed_shipment(self, pool):
        result = await query_shipment(pool, "ORD-0001")
        assert result["order_id"] == "ORD-0001"
        assert result["shipment_status"] == "delayed"
        assert result["data_source"] == "postgresql"

    async def test_days_delayed_exceeds_threshold(self, pool):
        """SHP-0001 estimated_delivery_date=2026-05-13; should be delayed > 3 days."""
        result = await query_shipment(pool, "ORD-0001")
        assert int(result["days_delayed"]) > 3, (
            f"Expected days_delayed > 3, got {result['days_delayed']}"
        )

    async def test_carrier_and_tracking_present(self, pool):
        result = await query_shipment(pool, "ORD-0001")
        assert result["carrier"] in ("UPS", "USPS", "FedEx", "DHL")
        assert result["tracking_number"]

    async def test_not_found_returns_error_dict(self, pool):
        result = await query_shipment(pool, "ORD-9999")
        assert "error" in result
