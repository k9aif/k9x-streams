# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""PaymentReleaseOrchestrator: runs the payment-release squad with live context.

A K9-AIF BaseOrchestrator given the context fabric's state (context_state=). For every
flow it names the entities the flow is about (live_context_refs); execute_squads() then
adds their current values as payload["live_context"] before the squad runs. It knows
squads only; the agent is registered at the entry point (app.py)."""

from __future__ import annotations

from typing import Any, Dict, List

from k9_aif_abb.k9_core.orchestration.base_orchestrator import BaseOrchestrator


class PaymentReleaseOrchestrator(BaseOrchestrator):
    layer = "K9X Streams PaymentReleaseOrchestrator"

    def __init__(self, squads: List[Any], **kwargs: Any):
        super().__init__(**kwargs)
        self.squads = squads

    def execute_flow(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = {**payload, "live_context_refs": [f"invoice:{payload['invoice_id']}"]}
        results = self.execute_squads(self.squads, payload)
        return {"status": "completed", **results}
