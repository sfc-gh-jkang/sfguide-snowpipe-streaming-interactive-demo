# Demo Assumptions & Architecture — Live Credit Desk
- **Last updated:** [Date]

This document captures the architectural decisions, physical constraints, and tradeoffs baked into the live ingest demo. Read this **before** the demo so you can answer "why X?" questions without hesitation.

---

## 1. Architecture (one diagram, end to end)

```
[ User clicks "Generate Trade" in Streamlit ]
            │
            ▼ HTTPS POST  (via Streamlit-on-Snowflake EAI through cloudflared tunnel)
[ Google VM — <your-vm-name> (us-central1-c) ]
   docker-compose:
     credit-ingest    (FastAPI 8080, X-API-Key auth)
       └─ StreamingService (4-channel pool, hash-by-position_id, self-healing)
            │ Snowpipe Streaming HPA Python SDK
            ▼
     observe-agent  (sidecar, OTLP relay → Observe.inc)
            │
            ▼  Authenticated via RSA keypair (CREDIT_INGEST_USR)
[ Snowflake — Snowpipe Streaming HPA service ]
   Auto-PIPE: RAW_EVENTS-STREAMING (created on first SDK channel open)
            │
            ▼  HPA server-side commit (Snowflake-controlled, see latency model below)
[ RAW_EVENTS — Static Interactive Table, CLUSTER BY (position_id, event_ts) ]
            │
            ▼  Sub-second SELECT via CREDIT_DEMO_INT_WH (Interactive Warehouse)
[ Streamlit on Snowflake — Container Runtime ]
   Live Tape  •  Latest Marks  •  Sector Exposure  •  Watchlist  •  Per-step latency badges
```

## 2. Why this shape (decisions and rationale)

### Why Snowpipe Streaming HPA, not INSERT?
- **Production realism.** The whole point of the demo is to show what a real producer pipeline (Aladdin export, Bloomberg PORT marks, S&P/Moody's webhooks) would look like wired to Snowflake. INSERT is an SE shortcut; HPA is the actual product.
- **Exactly-once delivery.** HPA gives offset tokens, ordered ingestion within a channel, schema validation server-side, and automatic backoff. INSERT gives none of these.
- **Throughput ceiling.** HPA scales to 10 GB/s/table. INSERT is per-statement and bottlenecks well before that.
- **Cost shape.** HPA is throughput-based ($credits per uncompressed GB). INSERT consumes a warehouse for the duration of the statement.

### Why a Google VM, not Streamlit-on-Snowflake (SiS) for the producer?
- **HPA SDK does NOT run inside Snowpark Container Services.** Per [Snowflake docs (Limitations and considerations)](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-limitations): *"Snowpark Container Services (SPCS) isn't supported."* SiS warehouse-mode runtime cannot make outbound network calls. SiS Container Runtime CAN reach external services via EAI, but it's not where you'd put a long-running streaming producer.
- **A real producer is a long-running service** with channel state, recovery logic, and metrics. It belongs on infrastructure that supports those needs. A GCP VM with docker-compose is the smallest realistic shape.
- **The VM also gives us the Observe-agent sidecar pattern** (mirrors the standard Observe sidecar pattern) — OTLP traces relayed via observe-agent on `localhost:4318`, no direct EAI to Observe needed.

### Why an Interactive Table directly (not standard table → Dynamic/derived IT)?
- **No 1-minute refresh hop.** Dynamic Tables and derived Interactive Tables have a `TARGET_LAG` floor of 60 seconds. If we wrote to a standard table and then refreshed an IT off it, click-to-visible would be bounded by ~60s + HPA commit (~7s) = up to 65s.
- **HPA writes server-side via the auto-PIPE**, bypassing the user-DML restriction on Interactive Tables. The IT is the source of truth, not a downstream copy.
- **Single layer = lowest possible HPA-to-query latency.** Result: end-to-end click-to-visible is bounded only by HPA's own commit window (~1-7s, see latency model).

### Why a static IT (no `TARGET_LAG`, no `AS query`)?
- A dynamic IT (with `TARGET_LAG`) refreshes from a source query. We don't have a source query — the data ARRIVES via HPA. So the IT must be static.
- We seed the schema with `AS SELECT … WHERE 1=0` (no rows, just types). The PIPE adds rows server-side; Snowflake treats this as a special server-driven path that's exempt from the IT's user-DML restriction.

### Why RSA keypair auth, not PAT?
- HPA requires keypair JWT. PATs and OAuth tokens are explicitly unsupported per docs.

### Why a separate service user (`CREDIT_INGEST_USR`) instead of a personal user?
- Service user has `TYPE = SERVICE` (no MFA, no browser SSO). Production-shape.
- Narrowly scoped role `CREDIT_INGEST_RL` with INSERT on RAW_EVENTS only — least privilege.

### Why cloudflared tunnel (not the laptop port-forward I tried earlier)?
- Streamlit on Snowflake can call out via External Access Integration only to **public, DNS-resolvable URLs**. It cannot reach `localhost:8080` on your laptop, and it cannot punch through IAP-tunneled SSH.
- The cloudflared tunnel turns the VM's `localhost:8080` into a stable public HTTPS URL with TLS termination at Cloudflare's edge — no need to open a public IP on the VM, no GCP firewall rules.
- We piggy-back on the existing `<your-vm-name>` VM (us-central1-c), already running cloudflared with named tunnels.

## 3. Latency model (the headline assumption)

### What the docs guarantee
Per [Snowflake's Snowpipe Streaming overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview):
> "As low as 5 second end-to-end ingest-to-query latency."

That's the **published SLA** for HPA. The 5 seconds is the floor, not a guarantee per-event. Sub-second click-to-visible is **physically impossible** with Snowpipe Streaming.

### Per-step breakdown (what we measure and surface in the UI)

| Step | What | Typical | Tunable? |
|------|------|---------|----------|
| **t0_click → t1_post_sent** | Streamlit serializes payload, makes HTTPS POST through EAI | 5-30ms | No (network) |
| **t1_post_sent → t1_vm_received** | Cloudflare edge → tunnel → VM FastAPI | 50-150ms | No (network) |
| **t1_vm_received → vm_sdk_appended** | StreamingService.stream_row(): hash partition, append_row to channel buffer | **0.2-2ms** | No (in-memory) |
| **vm_sdk_appended → vm_committed** | `wait_for_flush(timeout=10)`: SDK forces flush; HPA service commits to IT | **0.5-3s** | YES — this is where we optimize |
| **vm_committed → t2_post_returned** | FastAPI handler returns 200 with latency payload | 50-150ms | No (network) |
| **t2_post_returned → t3_streamlit_first_seen** | Streamlit polls `SELECT 1 FROM RAW_EVENTS WHERE event_id = ?` | 200-500ms | Slightly (polling cadence) |
| **t3_first_seen → t4_query_done** | Interactive Warehouse executes the dashboard tile query | <500ms | No (IT design optimal) |
| **End-to-end click → visible on dashboard** | Sum of above | **~1.5-4s** with optimizations | — |

### What we tuned to hit the lower bound
1. **`wait_for_flush()` after every `append_row()`** in `streaming_service.py`. Without this, the SDK buffers up to its `MAX_CLIENT_LAG` (default ~10s) before flushing — that's where the original 7s post-ack lag came from. Now the FastAPI handler doesn't return until the row is committed.
2. **`max_client_lag = 100 milliseconds`** in the SDK profile.json. Belt-and-braces if `wait_for_flush` is bypassed for any reason; SDK's own batch cadence is still aggressive.
3. **Channel pool of 4** sized to position-key hash. Avoids head-of-line blocking; per-position ordering preserved.
4. **3 warmup events fire at FastAPI startup** so the first real click doesn't pay cold-channel-open cost.
5. **Interactive Warehouse stays bound to RAW_EVENTS** (not auto-suspended during demo window). Sub-second query response from the very first click.
6. **HPA server-side commit cost** (~0.5-3s) is the only un-tunable component. This is the Snowflake SLA window.

### What we explicitly trade for low latency
- **Throughput.** Per-event `wait_for_flush` is anti-pattern for high-volume producers. A real production deployment would use the SDK's natural batching (`MAX_CLIENT_LAG=1-5s`) and accept the longer per-event latency to amortize commit cost across many rows. We say this in the talk track.
- **Cost.** Frequent flushes generate more HPA service calls. Negligible for click-driven demo; matters at production volume.
- **Generality.** The single-channel-flush pattern doesn't generalize to multi-tenant / multi-region producers. Use the SDK's batching there.

## 4. Network policy + auth assumptions

- **CREDIT_INGEST_USR** is allowed by `CREDIT_INGEST_POLICY` network rule (CIDR `<your-vm-static-ip>/32` — the static NAT IP of the GCP VM). If the VM's NAT IP changes (GCP regional outage, manual IP rotation), the policy must be updated.
- **The VM's docker-compose stack** mounts the keypair from `/opt/credit-ingest/keys/credit_ingest.p8` (chmod 600, owned by root). The container's appuser reads it via volume mount.
- **Streamlit-on-Snowflake** uses a separate role for the dashboard (read-only on RAW_EVENTS, POSITIONS_DIM, PORTFOLIO_LIVE_VIEW). No keypair needed — uses the SiS session.
- **Cloudflared tunnel auth** is one-way (Cloudflare → VM); the VM's API key (`X-API-Key: <set-via-env-INGEST_API_KEY>`) is the application-layer auth. Anyone with that key + the public URL can POST. Acceptable for demo; rotate via env-var for production.

## 5. Failure modes and recovery

| Failure | Impact | Recovery |
|---------|--------|----------|
| HPA channel token expires (~1h) | append_row raises | StreamingService `_is_recoverable` catches, `_recover_channel` reopens; bounded retry |
| Snowflake regional issue | All channels fail | StreamingService raises after MAX_RECOVERY_ATTEMPTS=3; FastAPI returns 503 |
| VM reboot or container restart | Channel state lost | `lifespan` reinitializes 4 channels + 3 warmup events on startup |
| Cloudflared tunnel disconnect | Streamlit sees 502 | cloudflared has `restart: always` in systemd; reconnects in 5-30s |
| Network policy IP drift | All ingest fails with 390195 | Update `CREDIT_INGEST_POLICY` network rule with new VM NAT IP |
| Interactive Warehouse cold | First query takes 3-5s | Pre-warm with `SELECT 1 FROM RAW_EVENTS LIMIT 1` 30s before the demo |
| Demo-day connection ID expired | Snowsight access blocked | `snow connection test -c <your-connection>` to refresh |

## 6. Production gaps (what we'd change before going live with a real customer dataset)

1. **Replace per-event `wait_for_flush` with batched flushing** (`MAX_CLIENT_LAG = 5 seconds`). Lower per-event latency for high-volume producers.
2. **Add `ResilientStreamingService` wrapper** with circuit breaker (the snowpipe-streaming skill provides the reference implementation). Today's StreamingService has retries but no circuit breaker — under sustained Snowflake pressure, requests will queue.
3. **Add Cortex Agent** to the Streamlit page that answers "what changed in the book in the last hour" / "which positions are at risk" in plain English.
4. **Wire real producers** instead of the synthetic generator: Aladdin daily export → Snowpipe Streaming worker; Bloomberg PORT marks via webhook → REST API; S&P/Moody's downgrades via API.
5. **Replace API key auth** with mTLS or OAuth client credentials. The X-API-Key is fine for demo, weak for production.
6. **DR**: Replication groups for the IT and the dimension table. Failover plan documented separately.
7. **Granular network policies** per-producer (not just per-VM) so we can audit which producer wrote which rows.

## 7. What we will NOT show (and how to deflect)

- **Multi-tenant isolation** — a real fund-of-funds deployment would partition by fund. We don't show this. *Deflect:* "Adding `fund_id` as a partition key on the channel pool is a one-line change."
- **Schema evolution mid-flight** — HPA supports automatic column addition, but we haven't demonstrated it. *Deflect:* "It's an opt-in feature on the auto-PIPE. We can show it in a follow-up POC."
- **Iceberg target** — HPA supports streaming into Iceberg v2/v3 tables. We chose a Snowflake-managed IT for sub-second query response. *Deflect:* "Iceberg is a trade-off — open table format for portability, slightly higher query latency. We can show that path if you have a Spark / Trino consumer."
- **Cost** — talk-track avoids dollar figures because they depend on contract structure and volume. *Deflect:* "We'd model this with your actual loan tape during a POC; ballpark is $50/month ingest + $1.5-3K Interactive Warehouse for a mid-size firm."

## 8. Sources

- [Snowpipe Streaming overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview)
- [HPA limitations](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-limitations)
- [HPA configurations](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-configurations)
- [HPA best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-best-practices)
- [Interactive Tables (CREATE)](https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table)
- Snowflake Quickstart "Snowpipe Streaming v2 → Interactive Tables"
- StreamingService reference implementation (from `snowpipe-streaming` Python SDK documentation)
