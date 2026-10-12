"""
Async client for the NovaSupport operational-data MCP server.

Creates a short-lived SSE connection per call — suitable for the demo load.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _server_url() -> str:
    try:
        from config.settings import settings
        return settings.mcp.operational_data_url
    except AttributeError:
        return "http://localhost:8001/sse"


class OperationalDataClient:
    """Calls the operational-data MCP server over SSE."""

    def __init__(self, server_url: str | None = None):
        self._url = server_url or _server_url()

    async def _call_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict:
        from mcp.client.sse import sse_client
        from mcp import ClientSession

        async with sse_client(self._url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)
                if result.isError:
                    raise RuntimeError(
                        f"MCP tool {tool_name!r} returned error: {result.content}"
                    )
                if not result.content:
                    raise RuntimeError(f"MCP tool {tool_name!r} returned empty content")
                return json.loads(result.content[0].text)

    async def get_customer(self, customer_id: str) -> dict:
        return await self._call_tool("get_customer", {"customer_id": customer_id})

    async def get_order(self, order_id: str) -> dict:
        return await self._call_tool("get_order", {"order_id": order_id})

    async def get_payment(self, order_id: str) -> dict:
        return await self._call_tool("get_payment", {"order_id": order_id})

    async def get_shipment(self, order_id: str) -> dict:
        return await self._call_tool("get_shipment", {"order_id": order_id})
