# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""Live feed to the browser: every event, MCP call and agent step is broadcast to the
connected pages (Server-Sent Events). Safe to call from any thread."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional, Set


class Hub:
    def __init__(self) -> None:
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.queues: Set[asyncio.Queue] = set()

    def publish(self, kind: str, **data: Any) -> None:
        msg = json.dumps({"kind": kind, "t": time.time(), **data}, default=str)
        if self.loop is None:
            return
        for q in list(self.queues):
            self.loop.call_soon_threadsafe(_put, q, msg)


def _put(q: asyncio.Queue, msg: str) -> None:
    if q.qsize() < 500:
        q.put_nowait(msg)


hub = Hub()
