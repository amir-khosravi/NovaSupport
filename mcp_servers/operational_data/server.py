"""
MCP operational-data server — read-only access to customers, orders, payments, shipments.

Run (SSE transport, port 8001):
    python -m mcp_servers.operational_data.server

The server exposes 4 tools:
    get_customer(customer_id)  → customer profile
    get_order(order_id)        → order + items + shipping_fee (joined from payments)
    get_payment(order_id)      → payment details
    get_shipment(order_id)     → shipment tracking + computed days_delayed
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import asyncpg
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


def _asyncpg_dsn() -> str:
    from config.settings import settings
    return settings.postgres.url.replace("postgresql+asyncpg://", "postgresql://")


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(_asyncpg_dsn(), min_size=1, max_size=5)
    return _pool


def _safe(v: Any) -> Any:
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def _row(record: asyncpg.Record | None) -> dict | None:
    if record is None:
        return None
    return {k: _safe(v) for k, v in dict(record).items()}


# ── Importable tool implementations (used directly by integration tests) ──────

async def query_customer(pool: asyncpg.Pool, customer_id: str) -> dict:
    record = await pool.fetchrow(
        """
        SELECT customer_id, full_name, email, account_status,
               preferred_language, created_at, risk_flag
        FROM customers
        WHERE customer_id = $1
        """,
        customer_id,
    )
    if record is None:
        return {"error": f"Customer {customer_id!r} not found", "customer_id": customer_id}
    d = _row(record)
    d["data_source"] = "postgresql"
    return d


async def query_order(pool: asyncpg.Pool, order_id: str) -> dict:
    record = await pool.fetchrow(
        """
        SELECT o.order_id, o.customer_id, o.order_date, o.order_status,
               o.total_amount, o.currency, o.shipping_method,
               o.expected_delivery_date, o.actual_delivery_date,
               COALESCE(p.shipping_fee, 0.0) AS shipping_fee
        FROM orders o
        LEFT JOIN payments p ON p.order_id = o.order_id
        WHERE o.order_id = $1
        """,
        order_id,
    )
    if record is None:
        return {"error": f"Order {order_id!r} not found", "order_id": order_id}
    d = _row(record)
    items = await pool.fetch(
        "SELECT product_id, product_name, quantity, unit_price FROM order_items WHERE order_id = $1",
        order_id,
    )
    d["items"] = [_row(item) for item in items]
    d["data_source"] = "postgresql"
    return d


async def query_payment(pool: asyncpg.Pool, order_id: str) -> dict:
    record = await pool.fetchrow(
        """
        SELECT payment_id, order_id, payment_status, payment_method,
               payment_amount, shipping_fee, paid_at, refund_status, duplicate_charge
        FROM payments
        WHERE order_id = $1
        """,
        order_id,
    )
    if record is None:
        return {"error": f"Payment for order {order_id!r} not found", "order_id": order_id}
    d = _row(record)
    d["data_source"] = "postgresql"
    return d


async def query_shipment(pool: asyncpg.Pool, order_id: str) -> dict:
    record = await pool.fetchrow(
        """
        SELECT shipment_id, order_id, carrier, tracking_number,
               shipment_status, last_location, shipped_at,
               estimated_delivery_date, delivered_at,
               CASE
                 WHEN shipment_status = 'delayed'
                      AND estimated_delivery_date IS NOT NULL
                      AND estimated_delivery_date < CURRENT_DATE
                 THEN (CURRENT_DATE - estimated_delivery_date)::int
                 ELSE 0
               END AS days_delayed
        FROM shipments
        WHERE order_id = $1
        """,
        order_id,
    )
    if record is None:
        return {"error": f"Shipment for order {order_id!r} not found", "order_id": order_id}
    d = _row(record)
    d["data_source"] = "postgresql"
    return d


# ── FastMCP server ─────────────────────────────────────────────────────────────

mcp = FastMCP(
    "novasupport-operational-data",
    instructions=(
        "Read-only access to NovaSupport operational data: "
        "customers, orders (with items), payments, and shipments."
    ),
    host="0.0.0.0",
    port=8001,
)


@mcp.tool()
async def get_customer(customer_id: str) -> dict:
    """Retrieve a customer profile by customer_id (e.g. 'C-0001')."""
    pool = await _get_pool()
    return await query_customer(pool, customer_id)


@mcp.tool()
async def get_order(order_id: str) -> dict:
    """Retrieve order details including line items and shipping_fee (e.g. 'ORD-0001')."""
    pool = await _get_pool()
    return await query_order(pool, order_id)


@mcp.tool()
async def get_payment(order_id: str) -> dict:
    """Retrieve payment record for an order (e.g. 'ORD-0001')."""
    pool = await _get_pool()
    return await query_payment(pool, order_id)


@mcp.tool()
async def get_shipment(order_id: str) -> dict:
    """Retrieve shipment tracking and computed days_delayed for an order."""
    pool = await _get_pool()
    return await query_shipment(pool, order_id)


if __name__ == "__main__":
    logger.info("Starting operational-data MCP server on 0.0.0.0:8001 (SSE transport)")
    mcp.run(transport="sse")
