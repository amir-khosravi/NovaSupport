-- NovaSupport PostgreSQL schema
-- Run directly or via scripts/seed_database.py

CREATE TABLE IF NOT EXISTS customers (
    customer_id        VARCHAR(20) PRIMARY KEY,
    full_name          VARCHAR(100) NOT NULL,
    email              VARCHAR(200) UNIQUE NOT NULL,
    account_status     VARCHAR(20) NOT NULL DEFAULT 'active',
    preferred_language VARCHAR(10) NOT NULL DEFAULT 'en',
    created_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    risk_flag          BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(20) PRIMARY KEY,
    name       VARCHAR(200) NOT NULL,
    price      NUMERIC(10, 2) NOT NULL,
    category   VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id               VARCHAR(20) PRIMARY KEY,
    customer_id            VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    order_date             TIMESTAMP NOT NULL,
    order_status           VARCHAR(30) NOT NULL,
    total_amount           NUMERIC(10, 2) NOT NULL,
    currency               VARCHAR(10) NOT NULL DEFAULT 'USD',
    shipping_method        VARCHAR(30) NOT NULL DEFAULT 'standard',
    expected_delivery_date DATE,
    actual_delivery_date   DATE
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id VARCHAR(20) PRIMARY KEY,
    order_id      VARCHAR(20) NOT NULL REFERENCES orders(order_id),
    product_id    VARCHAR(20),
    product_name  VARCHAR(200) NOT NULL,
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id       VARCHAR(20) PRIMARY KEY,
    order_id         VARCHAR(20) NOT NULL REFERENCES orders(order_id),
    payment_status   VARCHAR(30) NOT NULL,
    payment_method   VARCHAR(30) NOT NULL,
    payment_amount   NUMERIC(10, 2) NOT NULL,
    shipping_fee     NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    paid_at          TIMESTAMP,
    refund_status    VARCHAR(30) NOT NULL DEFAULT 'none',
    duplicate_charge BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id             VARCHAR(20) PRIMARY KEY,
    order_id                VARCHAR(20) NOT NULL REFERENCES orders(order_id),
    carrier                 VARCHAR(50),
    tracking_number         VARCHAR(50),
    shipment_status         VARCHAR(30) NOT NULL,
    last_location           VARCHAR(200),
    shipped_at              TIMESTAMP,
    estimated_delivery_date DATE,
    delivered_at            TIMESTAMP
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id   VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    order_id    VARCHAR(20) REFERENCES orders(order_id),
    message     TEXT NOT NULL,
    category    VARCHAR(50),
    priority    VARCHAR(20),
    status      VARCHAR(20) NOT NULL DEFAULT 'open',
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_states (
    workflow_id         TEXT        PRIMARY KEY,
    ticket_id           TEXT        NOT NULL,
    customer_id         TEXT,
    order_id            TEXT,
    status              TEXT        NOT NULL DEFAULT 'running',
    category            TEXT,
    recommended_action  TEXT,
    refund_amount       NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    final_response      TEXT,
    escalation_reason   TEXT,
    state_json          JSONB       NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
