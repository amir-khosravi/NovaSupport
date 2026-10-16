"""
Seed the NovaSupport PostgreSQL database from CSV files in data/synthetic/.

Usage:
    python scripts/seed_database.py

Requires PostgreSQL running:
    docker compose --profile db up -d postgres
"""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import date, datetime
from pathlib import Path

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data" / "synthetic"
SCHEMA_SQL = Path(__file__).parent.parent / "infrastructure" / "postgres" / "schema.sql"


def _asyncpg_dsn() -> str:
    from config.settings import settings
    return settings.postgres.url.replace("postgresql+asyncpg://", "postgresql://")


def _bool(v: str) -> bool:
    return v.strip().lower() in ("true", "1", "yes")


def _dt(v: str) -> datetime | None:
    v = v.strip()
    return datetime.fromisoformat(v) if v else None


def _date(v: str) -> date | None:
    v = v.strip()
    if not v:
        return None
    return datetime.fromisoformat(v).date()


async def apply_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    log.info("Schema applied")


async def truncate_all(conn: asyncpg.Connection) -> None:
    await conn.execute(
        "TRUNCATE support_tickets, shipments, payments, order_items, orders, products, customers CASCADE"
    )
    log.info("All tables truncated")


async def _seed_customers(conn: asyncpg.Connection) -> None:
    rows = list(csv.DictReader((DATA_DIR / "customers.csv").open(encoding="utf-8")))
    await conn.executemany(
        """
        INSERT INTO customers
            (customer_id, full_name, email, account_status, preferred_language, created_at, risk_flag)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (customer_id) DO NOTHING
        """,
        [
            (
                r["customer_id"], r["full_name"], r["email"],
                r["account_status"], r["preferred_language"],
                datetime.fromisoformat(r["created_at"]), _bool(r["risk_flag"]),
            )
            for r in rows
        ],
    )
    log.info("Seeded %d customers", len(rows))


async def _seed_products(conn: asyncpg.Connection) -> None:
    rows = list(csv.DictReader((DATA_DIR / "products.csv").open(encoding="utf-8")))
    await conn.executemany(
        "INSERT INTO products (product_id, name, price, category) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
        [(r["product_id"], r["name"], float(r["price"]), r["category"]) for r in rows],
    )
    log.info("Seeded %d products", len(rows))


async def _seed_orders(conn: asyncpg.Connection) -> None:
    rows = list(csv.DictReader((DATA_DIR / "orders.csv").open(encoding="utf-8")))
    await conn.executemany(
        """
        INSERT INTO orders
            (order_id, customer_id, order_date, order_status, total_amount,
             currency, shipping_method, expected_delivery_date, actual_delivery_date)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (order_id) DO NOTHING
        """,
        [
            (
                r["order_id"], r["customer_id"], datetime.fromisoformat(r["order_date"]),
                r["order_status"], float(r["total_amount"]), r["currency"],
                r["shipping_method"],
                _date(r["expected_delivery_date"]), _date(r["actual_delivery_date"]),
            )
            for r in rows
        ],
    )
    log.info("Seeded %d orders", len(rows))


async def _seed_order_items(conn: asyncpg.Connection) -> None:
    rows = list(csv.DictReader((DATA_DIR / "order_items.csv").open(encoding="utf-8")))
    await conn.executemany(
        """
        INSERT INTO order_items (order_item_id, order_id, product_id, product_name, quantity, unit_price)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (order_item_id) DO NOTHING
        """,
        [
            (r["order_item_id"], r["order_id"], r["product_id"],
             r["product_name"], int(r["quantity"]), float(r["unit_price"]))
            for r in rows
        ],
    )
    log.info("Seeded %d order_items", len(rows))


async def _seed_payments(conn: asyncpg.Connection) -> None:
    rows = list(csv.DictReader((DATA_DIR / "payments.csv").open(encoding="utf-8")))
    await conn.executemany(
        """
        INSERT INTO payments
            (payment_id, order_id, payment_status, payment_method, payment_amount,
             shipping_fee, paid_at, refund_status, duplicate_charge)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (payment_id) DO NOTHING
        """,
        [
            (
                r["payment_id"], r["order_id"], r["payment_status"],
                r["payment_method"], float(r["payment_amount"]), float(r["shipping_fee"]),
                _dt(r["paid_at"]), r["refund_status"], _bool(r["duplicate_charge"]),
            )
            for r in rows
        ],
    )
    log.info("Seeded %d payments", len(rows))


async def _seed_shipments(conn: asyncpg.Connection) -> None:
    rows = list(csv.DictReader((DATA_DIR / "shipments.csv").open(encoding="utf-8")))
    await conn.executemany(
        """
        INSERT INTO shipments
            (shipment_id, order_id, carrier, tracking_number, shipment_status,
             last_location, shipped_at, estimated_delivery_date, delivered_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (shipment_id) DO NOTHING
        """,
        [
            (
                r["shipment_id"], r["order_id"], r["carrier"],
                r["tracking_number"], r["shipment_status"],
                r["last_location"] or None,
                _dt(r["shipped_at"]), _date(r["estimated_delivery_date"]),
                _dt(r["delivered_at"]),
            )
            for r in rows
        ],
    )
    log.info("Seeded %d shipments", len(rows))


async def _seed_support_tickets(conn: asyncpg.Connection) -> None:
    rows = list(csv.DictReader((DATA_DIR / "support_tickets.csv").open(encoding="utf-8")))
    await conn.executemany(
        """
        INSERT INTO support_tickets
            (ticket_id, customer_id, order_id, message, category, priority, status, created_at, resolved_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (ticket_id) DO NOTHING
        """,
        [
            (
                r["ticket_id"], r["customer_id"], r["order_id"] or None,
                r["message"], r["category"] or None, r["priority"] or None,
                r["status"], datetime.fromisoformat(r["created_at"]), _dt(r["resolved_at"]),
            )
            for r in rows
        ],
    )
    log.info("Seeded %d support_tickets", len(rows))


async def main() -> None:
    dsn = _asyncpg_dsn()
    log.info("Connecting to %s", dsn.split("@", 1)[-1])
    conn = await asyncpg.connect(dsn)
    try:
        await apply_schema(conn)
        await truncate_all(conn)
        await _seed_customers(conn)
        await _seed_products(conn)
        await _seed_orders(conn)
        await _seed_order_items(conn)
        await _seed_payments(conn)
        await _seed_shipments(conn)
        await _seed_support_tickets(conn)
        log.info("Database seeding complete.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
