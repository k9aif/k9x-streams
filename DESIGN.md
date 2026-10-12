# K9X Streams — design note

Status: design, not built. Owner: Ravi Natarajan. Started 2026-10-12.

## Purpose

Agents decide on data. Most of what they read today is a snapshot: the payload they were
given, a file, or a retrieval index built hours or days ago. For many decisions that is
yesterday's data. K9X Streams gives agents **the current state of the things they reason
about, and what just changed, with a freshness guarantee**, and lets live events start agents.

Agents do not read Kafka. A model cannot consume a firehose; at decision time it needs
"the latest state of claim X, as of now" and "what changed in the last ten minutes".
K9X Streams turns event streams into that: governed, fresh, queryable context, served
over MCP.

## Where it sits

```
 live sources              K9X Streams                                    agents
 (Kafka / Redpanda,        ┌───────────────────────────────────────┐
  Confluent, CDC from  ──► │ 1. stream sources (adapters)          │
  databases, MQTT,         │ 2. live state store                   │
  webhooks)                │    latest value per entity + a short  │ ── pull ─► MCP tools / resources
                           │    window of changes, each with as_of │           (K9-AIF agents via
                           │ 3. triggers: rule on a stream ──►     │            MCPStreamableHttpConnector,
                           │    domain event                       │            or any MCP client)
                           │ 4. governance: freshness, scope,      │ ── push ─► domain event → Router → squad
                           │    masking                            │
                           └───────────────────────────────────────┘
```

Today the framework's Kafka carries the *workflow* (Router → Orchestrator → results, HIL
requests and replies). K9X Streams adds live *business data* to an agent's context. The
Enterprise Context Fabric decides what context an agent gets; K9X Streams keeps it current.

## Parts

1. **Stream sources.** One contract, several adapters, the framework's factory pattern
   (`Base<Concern>` → `<Provider>Adapter` → `<Concern>Factory`):
   - Kafka / Redpanda (the open default; runs on the current single-node Redpanda);
   - Confluent (Schema Registry, Flink SQL, managed connectors) for enterprises that have it;
   - change data capture from databases (Debezium), MQTT for devices, webhooks.
2. **Live state store.** A consumer keeps, per entity (invoice, claim, account, part), the
   latest value and a short window of recent changes, each stamped with `as_of` and its
   source event. Redis or PostgreSQL behind one store contract.
3. **Triggers (push).** A rule on a stream ("payment posted for an invoice under review",
   "inventory below threshold") publishes a domain event; the Router starts a squad exactly
   as events do today. No new execution path.
4. **MCP surface (pull).**
   - Tools: `get_current(entity, id)`, `recent_changes(entity, id, window)`,
     `query(entity, filter)`. Every answer carries `as_of` and the source.
   - Resources with subscriptions: `streams://<entity>/<id>`; a client subscribed to an
     invoice receives an "updated" notification when it changes, so a long-running squad
     can react mid-run without polling.

## Governance

K9-AIF's contribution; the same controls agents already have, applied to live data.

- **Untrusted input.** Stream values reach a prompt only as MCP tool results, which the
  framework screens with `tool_result_guard` (Shield, and Guardian when configured).
- **Freshness.** Every value has `as_of`; a caller or a check can require a maximum age and
  refuse stale data rather than decide on it.
- **Scope.** Zero Trust decides which agent may read which entity (identity from the
  framework's resolvers and authenticators); the MCP server authenticates its callers when
  it serves real data.
- **Masking.** PII is masked in the state store's answers before it leaves the server.
- **Audit.** Each answer records what was read, by whom, and as of when.

## First build: Accounts Payable on the existing MCP server

`k9x_mcp_server` (FastMCP, streamable HTTP) already serves six AP tools on synthetic JSON:
`retrieve_invoice_data`, `execute_three_way_match`, `post_gl_entry`, `screen_sanctions_list`,
`generate_payment_file`, `retrieve_expense_report`. Phase 1 makes them live:

1. A simulator publishes AP events to Redpanda: `ap.invoice.updated`, `ap.payment.posted`,
   `ap.vendor.flagged`.
2. A background consumer in the MCP server keeps live state per invoice, payment and vendor.
3. The existing tools answer from live state (with `as_of`); add `get_current`,
   `recent_changes`, and an invoice resource with subscriptions.
4. **The demo that shows why it matters:** while an AP squad checks an invoice, a
   `payment.posted` event arrives for it. With live state the agent sees "already paid, as of
   10 seconds ago" and stops a duplicate payment. With yesterday's data it pays twice.

The server stays intranet-only on synthetic data for Phase 1, so it keeps its current
no-authentication setup; authentication comes with real data or any tunnel exposure. Its
address is configuration (`MCP_SERVER_URL`), never written into code or docs.

## Phases

| Phase | What | Where |
|---|---|---|
| 1 | AP live state + MCP tools + duplicate-payment demo | `k9x_mcp_server` (SBB) + simulator here |
| 2 | Contracts promoted to the framework once proven: stream source, live state store, trigger | `k9-aif-framework` (ABB) |
| 3 | Confluent adapter; CDC adapter | adapters (SBB / framework OOB) |
| 4 | Freshness and scope checks as Shield / Zero Trust rules; catalog entries | framework |

Contracts are proven here first and promoted into the framework afterwards, the same
direction as Shield checks proven in K9X Satan.

## Open questions

- Store: Redis (fast, simple) or PostgreSQL (one less service; SQL queries) for Phase 1.
- Window length and retention per entity type.
- Partitioning when scaled: key by entity id so one entity's events stay in order
  (today's topics are single-partition, which already keeps order).
- Whether triggers live in K9X Streams or as a Router-side rule ABB.
