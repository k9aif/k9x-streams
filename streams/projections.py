# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""Accounts Payable projections: how each AP event changes the entities agents reason about.

This is the only domain code the context fabric needs (BaseContextProjection, K9-AIF 1.17).
A solution in another domain writes its own, one per business event."""

from __future__ import annotations

from typing import Any, Dict, List

from k9_aif_abb.k9_core.streams import BaseContextProjection, ContextUpdate, EventEnvelope


class InvoiceProjection(BaseContextProjection):
    """invoice.created / invoice.approved → the invoice's fields."""
    entity, id_field = "invoice", "invoice_id"

    def accepts(self, env: EventEnvelope) -> bool:
        return env.event_type.startswith("invoice.")

    def project(self, env: EventEnvelope) -> Dict[str, Any]:
        return {k: env.payload[k] for k in ("vendor_id", "vendor_name", "amount", "status", "due")
                if k in env.payload}


class PaymentProjection(BaseContextProjection):
    """payment.posted → the invoice is paid (payment appended) and the payment exists."""

    def accepts(self, env: EventEnvelope) -> bool:
        return env.event_type == "payment.posted"

    def project(self, env: EventEnvelope) -> Dict[str, Any]:
        return {}

    def updates(self, env: EventEnvelope) -> List[ContextUpdate]:
        p = env.payload
        payment = {"payment_id": p["payment_id"], "amount": p["amount"], "at": env.timestamp.isoformat(timespec="seconds")}
        return [ContextUpdate("invoice", p["invoice_id"], fields={"status": "paid"}, append={"payments": payment}),
                ContextUpdate("payment", p["payment_id"], fields={"invoice_id": p["invoice_id"], "amount": p["amount"]})]


class VendorFlagProjection(BaseContextProjection):
    """vendor.flagged → the vendor is flagged, with the list and reason."""

    def accepts(self, env: EventEnvelope) -> bool:
        return env.event_type == "vendor.flagged"

    def project(self, env: EventEnvelope) -> Dict[str, Any]:
        return {}

    def updates(self, env: EventEnvelope) -> List[ContextUpdate]:
        p = env.payload
        return [ContextUpdate("vendor", p["vendor_id"], fields={
            "vendor_name": p.get("vendor_name"), "status": "flagged",
            "flag": {"list": p.get("list"), "reason": p.get("reason")}})]


class VendorNoteProjection(BaseContextProjection):
    """vendor.note → the note is kept on the vendor (only notes the gate let through)."""

    def accepts(self, env: EventEnvelope) -> bool:
        return env.event_type == "vendor.note"

    def project(self, env: EventEnvelope) -> Dict[str, Any]:
        return {}

    def updates(self, env: EventEnvelope) -> List[ContextUpdate]:
        p = env.payload
        return [ContextUpdate("vendor", p["vendor_id"], fields={"vendor_name": p.get("vendor_name")},
                              append={"notes": p.get("note", "")}, defaults={"status": "ok"}, append_limit=5)]


AP_PROJECTIONS = [InvoiceProjection(), PaymentProjection(), VendorFlagProjection(), VendorNoteProjection()]
