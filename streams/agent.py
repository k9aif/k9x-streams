# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""PaymentReleaseAgent: a K9-AIF agent that decides whether to release payment for an
approved invoice, on live context from the context fabric.

It gets live context both ways the framework offers:
  1. injected: k9x Origin resolves the squad's declared live_context (config/origin.yaml)
     before the squad runs (BaseOrchestrator(origin=...));
  2. on demand: just before releasing, it re-checks over MCP (k9x Origin, called with
     the framework's MCPStreamableHttpConnector). Every MCP result is screened by k9x Shield
     (screen_tool_result): stream data is untrusted input.

Decision rules (deterministic, explainable):
  already paid (a payment posted)              -> HOLD: duplicate payment prevented
  vendor flagged (sanctions list)              -> HOLD: vendor flagged
  an event for it refused at the gate          -> HOLD for human review (quarantined)
  a result withheld by Shield                  -> HOLD for human review
  otherwise                                    -> RELEASE
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Callable, Dict, Optional

from k9_aif_abb.k9_core.agent.base_agent import BaseAgent
from k9_aif_abb.k9_core.integration.mcp_streamable_http_connector import MCPStreamableHttpConnector
from k9_aif_abb.k9_security.tool_result_guard import screen_tool_result

from streams import config


class PaymentReleaseAgent(BaseAgent):
    layer = "K9X Streams PaymentReleaseAgent"

    def __init__(self, config_: Optional[Dict[str, Any]] = None, governance: Any = None,
                 on_step: Optional[Callable[..., None]] = None):
        super().__init__(config_ or {}, governance=governance)
        self.shield = governance
        self.on_step = on_step or (lambda **_: None)
        self.mcp = MCPStreamableHttpConnector({"name": "k9x-streams", "kwargs": {"url": config.MCP_URL, "timeout": 15}})

    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return asyncio.run(self._review(payload))

    async def _call(self, run: str, step: str, tool: str, args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """One MCP call, screened by Shield. None when Shield withholds the result."""
        t0 = time.time()
        result = await self.mcp.call_tool(tool, args)
        ms = round((time.time() - t0) * 1000, 1)
        try:
            await screen_tool_result(self.shield, tool, result, {"agent": self.layer})
        except PermissionError as exc:
            self.on_step(run=run, step=step, tool=tool, args=args, ms=ms, screened="blocked",
                         reason=str(exc)[:300], result=None)
            return None
        self.on_step(run=run, step=step, tool=tool, args=args, ms=ms, screened="passed", result=result)
        return result

    async def _review(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        run = payload.get("run") or uuid.uuid4().hex[:8]
        inv_id = payload["invoice_id"]
        review_s = float(payload.get("review_seconds", 4))
        say = lambda text, **kw: self.on_step(run=run, step="note", text=text, **kw)

        say(f"Reviewing {inv_id} for payment release")
        inv = (payload.get("live_context") or {}).get(f"invoice:{inv_id}") or {"found": False}
        self.on_step(run=run, step="injected", tool="live_context", args={"ref": f"invoice:{inv_id}"},
                     screened="passed", result=inv)
        if not inv.get("found") or inv["value"].get("status") != "approved":
            return self._decide(run, inv_id, "HOLD", f"invoice is not approved (status: {inv.get('value', {}).get('status')})")
        vendor_id = inv["value"].get("vendor_id")

        say(f"Checking documents and amounts ({review_s:.0f} s)…", reviewing=review_s)
        await asyncio.sleep(review_s)

        say("Re-checking live state over MCP before releasing payment")
        inv = await self._call(run, "re-check the invoice", "get_current", {"entity": "invoice", "id": inv_id})
        changes = await self._call(run, "what changed while reviewing", "recent_changes",
                                   {"entity": "invoice", "id": inv_id, "window_seconds": 120})
        vendor = await self._call(run, "check the vendor", "get_current", {"entity": "vendor", "id": vendor_id})

        if inv is None or changes is None or vendor is None:
            return self._decide(run, inv_id, "HOLD", "a live-context answer was withheld by k9x Shield; held for human review")
        if inv["value"].get("status") == "paid" or inv["value"].get("payments"):
            p = (inv["value"].get("payments") or [{}])[-1]
            return self._decide(run, inv_id, "HOLD",
                                f"duplicate payment prevented: {p.get('payment_id', 'a payment')} already posted, "
                                f"{inv.get('age_seconds')} s ago (as of {inv.get('as_of')})")
        if vendor.get("found") and vendor["value"].get("status") == "flagged":
            flag = vendor["value"].get("flag") or {}
            return self._decide(run, inv_id, "HOLD", f"vendor flagged: {flag.get('reason')} ({flag.get('list')}), "
                                f"{vendor.get('age_seconds')} s ago")
        for entity in (inv, vendor):
            q = (entity.get("flags") or {}).get("quarantined")
            if q:
                who = inv["value"].get("vendor_name") if entity is vendor else None
                return self._decide(run, inv_id, "HOLD",
                                    f"a {q['event_type']} event for {who or entity['entity'] + ' ' + entity['id']} was refused at the "
                                    f"stream gate by k9x Shield ({q['blocked_by']}); held for human review")
        conflict = (inv.get("flags") or {}).get("conflict")
        if conflict:
            return self._decide(run, inv_id, "HOLD", f"conflicting data on the invoice: {conflict['detail']}")
        return self._decide(run, inv_id, "RELEASE",
                            f"approved, unpaid, vendor clear; live state as of {inv.get('as_of')}")

    def _decide(self, run: str, inv_id: str, decision: str, reason: str) -> Dict[str, Any]:
        self.on_step(run=run, step="decision", decision=decision, reason=reason, invoice_id=inv_id)
        return {"output": {"invoice_id": inv_id, "decision": decision, "reason": reason}, "run": run}
