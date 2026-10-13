# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""Consumption via Kafka: read the AP topics and fold every event into the live state.

A fresh consumer group per start, reading from the earliest offset, so the live state is
rebuilt from the topics after a restart (the topics are the source of truth)."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid

from aiokafka import AIOKafkaConsumer

from streams import config
from streams.hub import hub
from streams.state import store

log = logging.getLogger("k9x_streams.consumer")


async def run_consumer() -> None:
    while True:
        consumer = AIOKafkaConsumer(*config.TOPICS, **config.kafka_kwargs(),
                                    group_id=f"k9x-streams-state-{uuid.uuid4().hex[:6]}",
                                    auto_offset_reset="earliest",
                                    value_deserializer=lambda m: json.loads(m.decode("utf-8")))
        try:
            await consumer.start()
            hub.publish("kafka", connected=True, broker=config.KAFKA_BROKER)
            log.info("[Consumer] reading %s from %s", config.TOPICS, config.KAFKA_BROKER)
            async for msg in consumer:
                event = msg.value
                changed = store.apply(msg.topic, msg.offset, event)
                hub.publish("event", topic=msg.topic, offset=msg.offset, event=event,
                            changed=[f"{e}:{i}" for e, i in changed])
        except Exception as exc:   # broker down: say so, retry
            log.warning("[Consumer] %s; retrying in 5 s", exc)
            hub.publish("kafka", connected=False, error=str(exc)[:200])
            await asyncio.sleep(5)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass
