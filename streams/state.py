# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""Live state store: the latest value per entity and a short window of recent changes,
each stamped with as_of and the event it came from. Fed by the Kafka consumer, read by
the MCP tools. In memory for Phase 1; Redis or PostgreSQL behind the same methods later."""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

Key = Tuple[str, str]   # (entity, id): ("invoice", "INV-1001"), ("vendor", "V-03")


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


class LiveStateStore:
    WINDOW = 50   # changes kept per entity

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: Dict[Key, Dict[str, Any]] = {}
        self._changes: Dict[Key, deque] = {}
        self.counts: Dict[str, int] = {}
        self.listeners: List[Callable[[Dict[str, Any]], None]] = []

    # ── write side (consumer) ──────────────────────────────────────────
    def apply(self, topic: str, offset: int, event: Dict[str, Any]) -> List[Key]:
        """Fold one event into the state; return the entities it changed."""
        ts = time.time()
        src = {"topic": topic, "offset": offset, "event_id": event.get("event_id"), "type": event.get("type")}
        kind = event.get("type", "")
        changed: List[Key] = []
        with self._lock:
            self.counts[topic] = self.counts.get(topic, 0) + 1
            if kind.startswith("invoice."):
                inv = event["invoice_id"]
                fields = {k: event[k] for k in ("vendor_id", "vendor_name", "amount", "status", "due") if k in event}
                changed.append(self._set(("invoice", inv), fields, kind, ts, src))
            elif kind == "payment.posted":
                inv = event["invoice_id"]
                prev = (self._current.get(("invoice", inv)) or {}).get("value", {})
                payments = list(prev.get("payments", [])) + [
                    {"payment_id": event["payment_id"], "amount": event["amount"], "at": _iso(ts)}]
                changed.append(self._set(("invoice", inv), {"status": "paid", "payments": payments}, kind, ts, src))
                changed.append(self._set(("payment", event["payment_id"]),
                                         {"invoice_id": inv, "amount": event["amount"]}, kind, ts, src))
            elif kind == "vendor.flagged":
                changed.append(self._set(("vendor", event["vendor_id"]),
                                         {"vendor_name": event.get("vendor_name"), "status": "flagged",
                                          "flag": {"list": event.get("list"), "reason": event.get("reason")}},
                                         kind, ts, src))
            elif kind == "vendor.note":
                vid = event["vendor_id"]
                prev = (self._current.get(("vendor", vid)) or {}).get("value", {})
                notes = (list(prev.get("notes", [])) + [event.get("note", "")])[-5:]
                changed.append(self._set(("vendor", vid), {"vendor_name": event.get("vendor_name"), "notes": notes},
                                         kind, ts, src))
        return changed

    def _set(self, key: Key, fields: Dict[str, Any], kind: str, ts: float, src: Dict[str, Any]) -> Key:
        cur = self._current.get(key) or {"value": {}}
        value = {**cur["value"], **fields}
        if key[0] == "vendor":
            value.setdefault("status", "ok")
        self._current[key] = {"value": value, "as_of_ts": ts, "source": src}
        self._changes.setdefault(key, deque(maxlen=self.WINDOW)).append(
            {"at_ts": ts, "type": kind, "fields": fields, "source": src})
        return key

    # ── read side (MCP tools, UI) ──────────────────────────────────────
    def get_current(self, entity: str, entity_id: str) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            cur = self._current.get((entity, entity_id))
            if not cur:
                return {"entity": entity, "id": entity_id, "found": False, "as_of": None}
            return {"entity": entity, "id": entity_id, "found": True, "value": dict(cur["value"]),
                    "as_of": _iso(cur["as_of_ts"]), "age_seconds": round(now - cur["as_of_ts"], 1),
                    "source": dict(cur["source"])}

    def recent_changes(self, entity: str, entity_id: str, window_seconds: float = 600) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            items = [c for c in self._changes.get((entity, entity_id), []) if now - c["at_ts"] <= window_seconds]
        return {"entity": entity, "id": entity_id, "window_seconds": window_seconds, "as_of": _iso(now),
                "changes": [{"at": _iso(c["at_ts"]), "seconds_ago": round(now - c["at_ts"], 1), "type": c["type"],
                             "fields": c["fields"], "source": c["source"]} for c in items]}

    def query(self, entity: str, status: str = "", min_amount: float = 0) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            rows = [(k[1], v) for k, v in self._current.items() if k[0] == entity]
        out = []
        for eid, v in rows:
            val = v["value"]
            if status and val.get("status") != status:
                continue
            if min_amount and float(val.get("amount") or 0) < min_amount:
                continue
            out.append({"id": eid, "value": val, "as_of": _iso(v["as_of_ts"]),
                        "age_seconds": round(now - v["as_of_ts"], 1)})
        out.sort(key=lambda r: r["id"])
        return {"entity": entity, "as_of": _iso(now), "count": len(out), "rows": out}

    def snapshot(self) -> Dict[str, Any]:
        return {"invoices": self.query("invoice")["rows"], "vendors": self.query("vendor")["rows"],
                "counts": dict(self.counts)}


store = LiveStateStore()
