# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""The context fabric, assembled from K9-AIF building blocks (1.17):

    KafkaEventFabric → GovernanceEventGate (k9x Shield) → InMemoryContextState (AP projections)
                     → ContextMCPServer (get_current / recent_changes / query at /mcp)

Nothing here reads Kafka or stores state itself; it configures the framework's classes
through StreamsFactory and forwards what happens to the live page (hub)."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict

from k9_aif_abb.k9_factories.streams_factory import StreamsFactory
from k9_aif_abb.k9_mcp.servers.context_mcp_server import ContextMCPServer
from k9_aif_abb.k9_security.vulnerability.shield_governance import ShieldGovernance
from k9_aif_abb.k9_streams import ContextIngest

from streams import config
from streams.hub import hub
from streams.projections import AP_PROJECTIONS

log = logging.getLogger("k9x_streams.fabric")

FRAMEWORK_CONFIG: Dict[str, Any] = {
    "streams": {
        "enabled": True,
        "provider": "kafka",
        # Broker and SSL/SASL settings come from the environment (KAFKA_BROKER, KAFKA_SECURITY_PROTOCOL...).
        # earliest + no group: every start replays the topics, so the topics stay the source of truth.
        "kafka": {"auto_offset_reset": "earliest", "group_id": None},
        "state": {"max_age_seconds": config.MAX_AGE_SECONDS},
        "gate": "governance",
    },
    # k9x Shield for stream events and tool results: injection phrasing and
    # business-language payment diversion are refused wherever they appear.
    "security": {"shield": {"enabled": True,
                            "ingress": {"checks": ["PromptInjectionCheck", "PaymentDiversionCheck"]},
                            "egress": {"checks": ["PromptInjectionCheck"]}}},
}

shield = ShieldGovernance(FRAMEWORK_CONFIG)
state = StreamsFactory.create_state(FRAMEWORK_CONFIG, projections=AP_PROJECTIONS)
gate = StreamsFactory.create_gate(FRAMEWORK_CONFIG, governance=shield)
counts: Dict[str, int] = {}


def _on_event(e: Dict[str, Any]) -> None:
    env = e["envelope"]
    transport = (env.metadata or {}).get("transport") or {}
    if e["topic"]:
        counts[e["topic"]] = counts.get(e["topic"], 0) + 1
    hub.publish("event", topic=e["topic"], offset=transport.get("offset"),
                event={"type": env.event_type, "event_id": env.event_id, **env.payload},
                gate=e["decision"], reason=(e.get("reason") or "")[:240], changed=e["entities"])


ingest = ContextIngest(None, state, gate=gate, listeners=[_on_event])
mcp_server = ContextMCPServer(
    state, name="k9x-streams",
    instructions=("Live Accounts Payable state built from Kafka streams. Entities: invoice, vendor, payment. "
                  "Every result has as_of, age_seconds and stale; do not act on stale values."),
    on_call=lambda c: hub.publish("mcp", tool=c["tool"], args=c["args"], ms=c["ms"], as_of=c["as_of"],
                                  age=c["age_seconds"], found=c["found"]))
mcp = mcp_server.build_fastmcp()

_fabric = None
_lock = threading.Lock()


def fabric():
    """The Kafka event fabric (created on first use; the producer connects on first publish)."""
    global _fabric
    with _lock:
        if _fabric is None:
            _fabric = StreamsFactory.create_fabric(FRAMEWORK_CONFIG)
        return _fabric


def start_ingest(stop: threading.Event) -> None:
    """Attach the governed ingest to the AP topics; retry until the broker answers."""
    while not stop.is_set():
        try:
            ingest.fabric = fabric()
            ingest.attach(config.TOPICS)
            hub.publish("kafka", connected=True, broker=config.KAFKA_BROKER)
            log.info("[Fabric] ingesting %s from %s", config.TOPICS, config.KAFKA_BROKER)
            return
        except Exception as exc:
            log.warning("[Fabric] %s; retrying in 5 s", exc)
            hub.publish("kafka", connected=False, error=str(exc)[:200])
            time.sleep(5)


def close() -> None:
    if _fabric is not None:
        try:
            _fabric.close()
        except Exception:
            pass
