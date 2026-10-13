# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""Settings from the environment (.env); localhost defaults, no addresses in code."""

from __future__ import annotations

import os

# Broker and security settings are read by the framework's KafkaEventFabric from the same
# environment: KAFKA_BROKER (or KAFKA_BOOTSTRAP_SERVERS), KAFKA_SECURITY_PROTOCOL (PLAINTEXT,
# SSL, SASL_PLAINTEXT, SASL_SSL), KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD.
# Confluent Cloud is SASL_SSL + PLAIN with an API key and secret.
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BROKER", "localhost:9092")

PORT = int(os.getenv("STREAMS_PORT", "8780"))
# Where the agent reaches this service's MCP endpoint (the same process by default).
MCP_URL = os.getenv("STREAMS_MCP_URL", f"http://localhost:{PORT}/mcp")
# A value older than this is stale: the agent refuses to decide on it.
MAX_AGE_SECONDS = float(os.getenv("STREAMS_MAX_AGE_SECONDS", "120"))

TOPIC_INVOICE = os.getenv("STREAMS_TOPIC_INVOICE", "ap.invoice.updated")
TOPIC_PAYMENT = os.getenv("STREAMS_TOPIC_PAYMENT", "ap.payment.posted")
TOPIC_VENDOR = os.getenv("STREAMS_TOPIC_VENDOR", "ap.vendor.flagged")
TOPICS = [TOPIC_INVOICE, TOPIC_PAYMENT, TOPIC_VENDOR]
