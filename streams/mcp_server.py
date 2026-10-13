# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""The MCP surface: agents ask for live context here, never Kafka directly.

Every answer carries as_of (when the value last changed), age_seconds and the source
event (topic, offset), so the caller can judge freshness and trace provenance."""

from __future__ import annotations

import time
from typing import Any, Dict

from mcp.server.fastmcp import FastMCP

from streams.hub import hub
from streams.state import store

mcp = FastMCP("k9x-streams", instructions=(
    "Live Accounts Payable state built from Kafka streams. Entities: invoice, vendor, payment. "
    "Every result has as_of and age_seconds; treat old values as stale."), stateless_http=True)


def _traced(tool: str, args: Dict[str, Any], fn) -> Dict[str, Any]:
    t0 = time.time()
    out = fn()
    hub.publish("mcp", tool=tool, args=args, ms=round((time.time() - t0) * 1000, 1),
                as_of=out.get("as_of"), age=out.get("age_seconds"), found=out.get("found", True))
    return out


@mcp.tool()
def get_current(entity: str, id: str) -> Dict[str, Any]:
    """Latest state of one entity (invoice, vendor or payment), with as_of and its source event."""
    return _traced("get_current", {"entity": entity, "id": id}, lambda: store.get_current(entity, id))


@mcp.tool()
def recent_changes(entity: str, id: str, window_seconds: float = 600) -> Dict[str, Any]:
    """What changed for one entity within the last window_seconds, newest last."""
    return _traced("recent_changes", {"entity": entity, "id": id, "window_seconds": window_seconds},
                   lambda: store.recent_changes(entity, id, window_seconds))


@mcp.tool()
def query(entity: str, status: str = "", min_amount: float = 0) -> Dict[str, Any]:
    """Entities of one type, optionally by status and minimum amount (e.g. approved invoices over 10000)."""
    return _traced("query", {"entity": entity, "status": status, "min_amount": min_amount},
                   lambda: store.query(entity, status, min_amount))


@mcp.resource("streams://{entity}/{id}")
def entity_resource(entity: str, id: str) -> Dict[str, Any]:
    """The same live state as get_current, as an MCP resource."""
    return store.get_current(entity, id)
