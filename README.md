# K9X Streams

Real-time, governed context for agents built on [K9-AIF](https://github.com/k9aif/k9-aif-framework).

Agents need the current state of what they reason about, not yesterday's snapshot.
K9X Streams turns event streams (Kafka / Redpanda, Confluent, change data capture) into a
live state store and serves it over MCP: the latest value per entity and what just changed,
each with an `as_of` timestamp, under the framework's governance (Shield screening of tool
results, Zero Trust scope, PII masking). Rules on a stream can also start agents through the
K9-AIF Router.

Status: design. See [DESIGN.md](DESIGN.md).

Part of the K9X ecosystem · Apache-2.0
