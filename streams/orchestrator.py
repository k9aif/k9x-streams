# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""PaymentReleaseOrchestrator: runs the payment-release squad with live context.

A K9-AIF BaseOrchestrator given k9x Origin (origin=). The squad declares what it needs in
config/origin.yaml (live_context: ["invoice:{invoice_id}"]); execute_squads() resolves it
for each payload before the squad runs. It knows squads only; the agent is registered at
the entry point (app.py)."""

from __future__ import annotations

from typing import Any, Dict, List

from k9_aif_abb.k9_core.orchestration.base_orchestrator import BaseOrchestrator


class PaymentReleaseOrchestrator(BaseOrchestrator):
    layer = "K9X Streams PaymentReleaseOrchestrator"

    def __init__(self, squads: List[Any], **kwargs: Any):
        super().__init__(**kwargs)
        self.squads = squads

    def execute_flow(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        results = self.execute_squads(self.squads, payload)
        return {"status": "completed", **results}
