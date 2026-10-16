"""
Unit tests for the 4 MCP-backed worker handlers.

All MCP calls are mocked — no network or database required.
Tests verify:
  - Correct field mapping (DB names → expected API contract)
  - Mock fallback when OperationalDataClient raises
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from apps.worker.handlers import registry as default_registry
from streaming.schemas import AgentCommand, TaskType, new_id


def _cmd(task_type: TaskType, payload: dict | None = None) -> AgentCommand:
    return AgentCommand(
        workflow_id=new_id(),
        ticket_id="TKT-UNIT-001",
        task_type=task_type,
        payload=payload or {},
        correlation_id=new_id(),
    )


# ── GET_ORDER ─────────────────────────────────────────────────────────────────

class TestGetOrderHandler:

    async def test_maps_total_amount_to_order_total(self):
        """DB returns total_amount; graph expects order_total."""
        mcp_response = {
            "order_id": "ORD-0001",
            "customer_id": "C-0020",
            "order_date": "2026-05-05T03:35:06",
            "order_status": "in_transit",
            "total_amount": 282.96,
            "currency": "USD",
            "shipping_method": "express",
            "shipping_fee": 24.99,
            "items": [{"product_id": "P001", "product_name": "Wireless Headphones", "quantity": 3, "unit_price": 89.99}],
            "data_source": "postgresql",
        }
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_order.return_value = mcp_response
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_ORDER, {"order_id": "ORD-0001", "customer_id": "C-0020"})
            )

        assert result["order_id"] == "ORD-0001"
        assert result["shipping_method"] == "express"
        assert result["order_total"] == pytest.approx(282.96)
        assert result["shipping_fee"] == pytest.approx(24.99)
        assert result["data_source"] == "postgresql"
        inst.get_order.assert_called_once_with("ORD-0001")

    async def test_falls_back_to_mock_on_mcp_error(self):
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_order.side_effect = RuntimeError("connection refused")
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_ORDER, {"order_id": "ORD-0001"})
            )

        assert result["data_source"] == "mock_mcp"
        assert result["shipping_fee"] == pytest.approx(24.99)
        assert result["shipping_method"] == "express"

    async def test_falls_back_on_not_found(self):
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_order.return_value = {"error": "Order 'ORD-9999' not found", "order_id": "ORD-9999"}
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_ORDER, {"order_id": "ORD-9999"})
            )

        assert result["data_source"] == "mock_mcp"


# ── GET_CUSTOMER ──────────────────────────────────────────────────────────────

class TestGetCustomerHandler:

    async def test_maps_full_name_to_name_and_account_status_to_tier(self):
        """DB returns full_name/account_status; graph reads name/tier."""
        mcp_response = {
            "customer_id": "C-0001",
            "full_name": "Alice Smith",
            "email": "alice.smith@example.com",
            "account_status": "vip",
            "preferred_language": "en",
            "created_at": "2022-02-08T00:00:00",
            "risk_flag": False,
            "data_source": "postgresql",
        }
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_customer.return_value = mcp_response
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_CUSTOMER, {"customer_id": "C-0001"})
            )

        assert result["name"] == "Alice Smith"      # graph reads "name"
        assert result["full_name"] == "Alice Smith"
        assert result["tier"] == "vip"              # graph reads "tier"
        assert result["email"] == "alice.smith@example.com"
        assert result["data_source"] == "postgresql"

    async def test_falls_back_to_mock_on_mcp_error(self):
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_customer.side_effect = ConnectionError("MCP server down")
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_CUSTOMER, {"customer_id": "C-0001"})
            )

        assert result["data_source"] == "mock_mcp"
        assert result["tier"] == "gold"


# ── GET_SHIPMENT ──────────────────────────────────────────────────────────────

class TestGetShipmentHandler:

    async def test_passes_through_days_delayed(self):
        mcp_response = {
            "shipment_id": "SHP-0001",
            "order_id": "ORD-0001",
            "carrier": "UPS",
            "tracking_number": "UP784095277",
            "shipment_status": "delayed",
            "last_location": "New York",
            "shipped_at": "2026-05-05T07:35:06",
            "estimated_delivery_date": "2026-05-13",
            "delivered_at": None,
            "days_delayed": 26,
            "data_source": "postgresql",
        }
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_shipment.return_value = mcp_response
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_SHIPMENT, {"order_id": "ORD-0001"})
            )

        assert result["shipment_status"] == "delayed"
        assert result["days_delayed"] == 26
        assert result["carrier"] == "UPS"
        assert result["data_source"] == "postgresql"

    async def test_falls_back_to_mock_on_mcp_error(self):
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_shipment.side_effect = TimeoutError("MCP timeout")
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_SHIPMENT, {"order_id": "ORD-0001"})
            )

        assert result["data_source"] == "mock_mcp"
        assert result["days_delayed"] == 5


# ── GET_PAYMENT ───────────────────────────────────────────────────────────────

class TestGetPaymentHandler:

    async def test_maps_payment_amount_to_amount_charged(self):
        """DB returns payment_amount; handler exposes amount_charged for compatibility."""
        mcp_response = {
            "payment_id": "PAY-0001",
            "order_id": "ORD-0001",
            "payment_status": "completed",
            "payment_method": "paypal",
            "payment_amount": 282.96,
            "shipping_fee": 24.99,
            "paid_at": "2026-05-05T03:35:06",
            "refund_status": "none",
            "duplicate_charge": True,
            "data_source": "postgresql",
        }
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_payment.return_value = mcp_response
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_PAYMENT, {"order_id": "ORD-0001"})
            )

        assert result["payment_status"] == "completed"
        assert result["amount_charged"] == pytest.approx(282.96)
        assert result["shipping_fee"] == pytest.approx(24.99)
        assert result["duplicate_charge"] is True
        assert result["data_source"] == "postgresql"

    async def test_falls_back_to_mock_on_mcp_error(self):
        with patch("mcp_servers.client.OperationalDataClient") as MockCls:
            inst = AsyncMock()
            inst.get_payment.side_effect = RuntimeError("server error")
            MockCls.return_value = inst

            result = await default_registry.dispatch(
                _cmd(TaskType.GET_PAYMENT, {"order_id": "ORD-0001"})
            )

        assert result["data_source"] == "mock_mcp"
        assert result["payment_status"] == "completed"
