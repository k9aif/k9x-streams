# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""Settings from the environment (.env); localhost defaults, no addresses in code."""

from __future__ import annotations

import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# Organization brokers: Apache Kafka, Confluent Platform / Cloud, Redpanda, Amazon MSK...
# PLAINTEXT (default), SSL, SASL_PLAINTEXT or SASL_SSL; Confluent Cloud is SASL_SSL + PLAIN
# with an API key and secret as username and password.
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")


def kafka_kwargs() -> dict:
    """Connection settings shared by the producer and the consumer."""
    kw = {"bootstrap_servers": KAFKA_BROKER, "security_protocol": KAFKA_SECURITY_PROTOCOL}
    if KAFKA_SECURITY_PROTOCOL in ("SSL", "SASL_SSL"):
        import ssl
        kw["ssl_context"] = ssl.create_default_context()
    if KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
        kw.update(sasl_mechanism=KAFKA_SASL_MECHANISM, sasl_plain_username=KAFKA_SASL_USERNAME,
                  sasl_plain_password=KAFKA_SASL_PASSWORD)
    return kw


PORT = int(os.getenv("STREAMS_PORT", "8780"))
# Where the agent reaches this service's MCP endpoint (the same process by default).
MCP_URL = os.getenv("STREAMS_MCP_URL", f"http://localhost:{PORT}/mcp")
# A value older than this is stale: the agent refuses to decide on it.
MAX_AGE_SECONDS = float(os.getenv("STREAMS_MAX_AGE_SECONDS", "120"))

TOPIC_INVOICE = os.getenv("STREAMS_TOPIC_INVOICE", "ap.invoice.updated")
TOPIC_PAYMENT = os.getenv("STREAMS_TOPIC_PAYMENT", "ap.payment.posted")
TOPIC_VENDOR = os.getenv("STREAMS_TOPIC_VENDOR", "ap.vendor.flagged")
TOPICS = [TOPIC_INVOICE, TOPIC_PAYMENT, TOPIC_VENDOR]
