"""
Phase 1 — Step 3: Generate synthetic operational data for MCP.

Creates small, realistic demo datasets:
  - 20 customers
  - 50 orders  (with items)
  - 50 payments
  - 50 shipments
  - 30 support tickets (matching the delayed-order scenario)

Saves to data/synthetic/ as CSV files for seeding PostgreSQL later.

Run: python scripts/generate_operational_data.py
"""

import csv
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SYNTHETIC_DIR = ROOT / "data" / "synthetic"
SYNTHETIC_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)

# ──────────────────────────────────────────────────────────────────────────────
# Reference data
# ──────────────────────────────────────────────────────────────────────────────

FIRST_NAMES = ["Alice", "Bob", "Carlos", "Diana", "Ethan", "Fatima", "George",
               "Hannah", "Ivan", "Julia", "Kevin", "Laura", "Mohammed", "Nina",
               "Oscar", "Paula", "Raj", "Sara", "Tom", "Uma"]

LAST_NAMES = ["Smith", "Johnson", "Garcia", "Brown", "Lee", "Patel", "Wilson",
              "Martinez", "Taylor", "Anderson", "Thomas", "Jackson", "White",
              "Harris", "Clark", "Lewis", "Walker", "Hall", "Young", "King"]

PRODUCTS = [
    {"product_id": "P001", "name": "Wireless Headphones", "price": 89.99, "category": "Electronics"},
    {"product_id": "P002", "name": "Running Shoes", "price": 64.99, "category": "Footwear"},
    {"product_id": "P003", "name": "Coffee Maker", "price": 49.99, "category": "Appliances"},
    {"product_id": "P004", "name": "Yoga Mat", "price": 29.99, "category": "Sports"},
    {"product_id": "P005", "name": "Laptop Stand", "price": 39.99, "category": "Electronics"},
    {"product_id": "P006", "name": "Winter Jacket", "price": 119.99, "category": "Clothing"},
    {"product_id": "P007", "name": "Blender", "price": 55.00, "category": "Appliances"},
    {"product_id": "P008", "name": "Desk Lamp", "price": 24.99, "category": "Home"},
]

CARRIERS = ["FedEx", "UPS", "DHL", "USPS"]
SHIPPING_METHODS = ["standard", "express"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
          "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin"]


def make_id(prefix: str, n: int) -> str:
    return f"{prefix}-{n:04d}"


def rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows)} rows -> {path.relative_to(ROOT)}")


# ──────────────────────────────────────────────────────────────────────────────
# Generators
# ──────────────────────────────────────────────────────────────────────────────

def generate_customers(n: int = 20) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        first = FIRST_NAMES[i - 1]
        last = LAST_NAMES[i - 1]
        # 2 VIP, 2 flagged, rest normal
        if i <= 2:
            account_status = "vip"
        elif i in (19, 20):
            account_status = "flagged"
        else:
            account_status = "active"

        customers.append({
            "customer_id": make_id("C", i),
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}@example.com",
            "account_status": account_status,
            "preferred_language": random.choice(["en", "en", "en", "es", "fr"]),
            "created_at": rand_date(
                datetime(2022, 1, 1), datetime(2024, 6, 1)
            ).strftime("%Y-%m-%d"),
            "risk_flag": account_status == "flagged",
        })
    return customers


def generate_orders(customers: list[dict], n: int = 50) -> tuple[list[dict], list[dict]]:
    orders = []
    order_items = []
    now = datetime.now()

    for i in range(1, n + 1):
        customer = random.choice(customers)
        order_date = rand_date(now - timedelta(days=60), now - timedelta(days=1))

        # Orders 1-10: forced express + in_transit → main demo scenario (delayed express)
        # Orders 11-15: standard + in_transit → secondary delay scenario
        # Rest: random
        if i <= 10:
            shipping_method = "express"
        elif i <= 15:
            shipping_method = "standard"
        else:
            shipping_method = random.choice(SHIPPING_METHODS)

        # Determine status — skew toward delayed/in-transit for demo realism
        if i <= 15:
            # These orders will be delayed — main demo scenario
            status = "in_transit"
            expected = order_date + timedelta(days=2 if shipping_method == "express" else 7)
            actual = None  # not delivered yet
        elif i <= 30:
            status = "delivered"
            expected = order_date + timedelta(days=2 if shipping_method == "express" else 7)
            actual = expected + timedelta(days=random.randint(-1, 2))
        elif i <= 40:
            status = "processing"
            expected = order_date + timedelta(days=2 if shipping_method == "express" else 7)
            actual = None
        else:
            status = "cancelled"
            expected = order_date + timedelta(days=2 if shipping_method == "express" else 7)
            actual = None

        product = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        item_total = round(product["price"] * qty, 2)
        shipping_fee = 12.99 if shipping_method == "express" else (0.0 if item_total >= 50 else 4.99)
        total = round(item_total + shipping_fee, 2)

        order_id = make_id("ORD", i)
        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "order_status": status,
            "total_amount": total,
            "currency": "USD",
            "shipping_method": shipping_method,
            "expected_delivery_date": expected.strftime("%Y-%m-%d"),
            "actual_delivery_date": actual.strftime("%Y-%m-%d") if actual else None,
        })

        order_items.append({
            "order_item_id": make_id("OI", i),
            "order_id": order_id,
            "product_id": product["product_id"],
            "product_name": product["name"],
            "quantity": qty,
            "unit_price": product["price"],
        })

    return orders, order_items


def generate_payments(orders: list[dict]) -> list[dict]:
    payments = []
    methods = ["credit_card", "credit_card", "paypal", "debit_card", "apple_pay"]

    for i, order in enumerate(orders, start=1):
        if order["order_status"] == "cancelled":
            pay_status = "refunded"
            refund_status = "completed"
        elif order["order_status"] == "delivered":
            pay_status = "completed"
            refund_status = "none"
        else:
            pay_status = "completed"
            refund_status = "none"

        # Orders 1-5: add a duplicate charge scenario for demo
        has_duplicate = i <= 3

        payments.append({
            "payment_id": make_id("PAY", i),
            "order_id": order["order_id"],
            "payment_status": pay_status,
            "payment_method": random.choice(methods),
            "payment_amount": order["total_amount"],
            "shipping_fee": 12.99 if order["shipping_method"] == "express" else 4.99,
            "paid_at": order["order_date"],
            "refund_status": refund_status,
            "duplicate_charge": has_duplicate,
        })

    return payments


def generate_shipments(orders: list[dict]) -> list[dict]:
    shipments = []
    now = datetime.now()

    for i, order in enumerate(orders, start=1):
        if order["order_status"] == "cancelled":
            ship_status = "cancelled"
            last_location = None
        elif order["order_status"] == "delivered":
            ship_status = "delivered"
            last_location = random.choice(CITIES)
        elif order["order_status"] == "in_transit":
            # First 5: significantly delayed (>3 business days past expected)
            # Next 10: slightly delayed
            if i <= 5:
                ship_status = "delayed"
                last_location = random.choice(CITIES)
            else:
                ship_status = "in_transit"
                last_location = random.choice(CITIES)
        else:
            ship_status = "processing"
            last_location = None

        order_date = datetime.strptime(order["order_date"], "%Y-%m-%d %H:%M:%S")
        shipped_at = order_date + timedelta(hours=random.randint(4, 24))
        expected = datetime.strptime(order["expected_delivery_date"], "%Y-%m-%d")

        # For delayed shipments, estimated delivery is pushed back
        if ship_status == "delayed":
            estimated = expected + timedelta(days=random.randint(4, 7))
        elif ship_status == "delivered":
            estimated = expected
        else:
            estimated = expected

        delivered_at = None
        if order["actual_delivery_date"]:
            delivered_at = order["actual_delivery_date"]

        carrier = random.choice(CARRIERS)
        tracking = f"{carrier[:2].upper()}{random.randint(100000000, 999999999)}"

        shipments.append({
            "shipment_id": make_id("SHP", i),
            "order_id": order["order_id"],
            "carrier": carrier,
            "tracking_number": tracking,
            "shipment_status": ship_status,
            "last_location": last_location,
            "shipped_at": shipped_at.strftime("%Y-%m-%d %H:%M:%S"),
            "estimated_delivery_date": estimated.strftime("%Y-%m-%d"),
            "delivered_at": delivered_at,
        })

    return shipments


def generate_tickets(orders: list[dict], customers: list[dict]) -> list[dict]:
    """30 support tickets — mix of scenarios matching our workflow categories."""
    tickets = []
    customer_map = {c["customer_id"]: c for c in customers}

    # Scenario templates — realistic messages per category
    scenarios = [
        # (category, priority, message_template)
        # NOTE: delivery_delay messages are generated dynamically based on actual shipping_method
        ("delivery_delay", "high", "__dynamic__"),
        ("delivery_delay", "medium",
         "Where is my order {order_id}? The tracking hasn't updated in 5 days."),
        ("request_refund", "high",
         "I want a refund for order {order_id}. The item arrived damaged and I've already returned it."),
        ("request_refund", "medium",
         "I cancelled order {order_id} but haven't seen the refund yet. It's been a week."),
        ("payment_problem", "high",
         "I was charged twice for order {order_id}. Please investigate and refund the duplicate charge."),
        ("payment_problem", "medium",
         "My payment for order {order_id} failed but I can see a pending charge. Is this normal?"),
        ("damaged_product", "high",
         "The product in order {order_id} arrived broken. The packaging was clearly damaged during shipping."),
        ("cancel_order", "low",
         "Can I cancel order {order_id}? I placed it by mistake."),
        ("account_issue", "low",
         "I can't log in to my account. I'm trying to check the status of order {order_id}."),
        ("product_question", "low",
         "I want to know if the item in order {order_id} comes with a warranty."),
    ]

    for i in range(1, 31):
        scenario = scenarios[(i - 1) % len(scenarios)]
        category, priority, msg_template = scenario

        # Link to a relevant order
        order = orders[i - 1] if i <= len(orders) else orders[-1]
        customer = customer_map[order["customer_id"]]

        order_date = datetime.strptime(order["order_date"], "%Y-%m-%d %H:%M:%S")
        days_late = (datetime.now() - datetime.strptime(
            order["expected_delivery_date"], "%Y-%m-%d"
        )).days
        days_late = max(days_late, 1)

        if msg_template == "__dynamic__":
            # Build message that accurately reflects the order's actual shipping method
            if order["shipping_method"] == "express":
                message = (
                    f"My order {order['order_id']} hasn't arrived. "
                    f"I paid for express delivery and it's already {days_late} days late. "
                    f"Can I get a refund on the shipping fee?"
                )
            else:
                message = (
                    f"My order {order['order_id']} hasn't arrived. "
                    f"It's {days_late} days past the expected delivery date. "
                    f"What is the status and what can be done?"
                )
        else:
            message = msg_template.format(order_id=order["order_id"], days=days_late)

        created = rand_date(order_date + timedelta(days=3), datetime.now())

        tickets.append({
            "ticket_id": make_id("TKT", i),
            "customer_id": customer["customer_id"],
            "order_id": order["order_id"],
            "message": message,
            "category": category,
            "priority": priority,
            "status": "open",
            "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
            "resolved_at": None,
        })

    return tickets


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("NovaSupport — Phase 1: Generating synthetic operational data\n")

    customers = generate_customers(20)
    orders, order_items = generate_orders(customers, 50)
    payments = generate_payments(orders)
    shipments = generate_shipments(orders)
    tickets = generate_tickets(orders, customers)

    write_csv(SYNTHETIC_DIR / "customers.csv", customers)
    write_csv(SYNTHETIC_DIR / "orders.csv", orders)
    write_csv(SYNTHETIC_DIR / "order_items.csv", order_items)
    write_csv(SYNTHETIC_DIR / "payments.csv", payments)
    write_csv(SYNTHETIC_DIR / "shipments.csv", shipments)
    write_csv(SYNTHETIC_DIR / "support_tickets.csv", tickets)

    # Save products reference
    write_csv(SYNTHETIC_DIR / "products.csv", PRODUCTS)

    # Quick summary
    print("\nSummary:")
    print(f"  Customers      : {len(customers)} (2 VIP, 2 flagged, 16 active)")
    print(f"  Orders         : {len(orders)}")

    status_counts = {}
    for o in orders:
        status_counts[o["order_status"]] = status_counts.get(o["order_status"], 0) + 1
    for status, count in sorted(status_counts.items()):
        print(f"    {status:<15}: {count}")

    delayed = sum(1 for s in shipments if s["shipment_status"] == "delayed")
    print(f"  Delayed ships  : {delayed} (main demo scenario)")
    dup = sum(1 for p in payments if p["duplicate_charge"])
    print(f"  Duplicate charges: {dup} (payment demo scenario)")
    print(f"  Tickets        : {len(tickets)}")

    cat_counts = {}
    for t in tickets:
        cat_counts[t["category"]] = cat_counts.get(t["category"], 0) + 1
    for cat, count in sorted(cat_counts.items()):
        print(f"    {cat:<25}: {count}")

    print("\nDone. Next step: scripts/seed_database.py (Phase 12)")
    print("Policy files are ready in data/knowledge/ for RAG ingestion (Phase 8)")


if __name__ == "__main__":
    main()
