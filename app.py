"""ACME Credit Management — Live Credit Desk Demo.

Streamlit Container Runtime app for Snowflake.
Fires events via VM-hosted Snowpipe Streaming HPA worker,
displays per-step latency, and queries Interactive Table for sub-second analytics.
Includes Cortex Agent chat tab for conversational analytics.
"""

import os
import time
import json

import streamlit as st
import pandas as pd
import plotly.express as px
import requests

import queries

# ---------------------------------------------------------------------------
# Session — Container Runtime provides get_active_session()
# ---------------------------------------------------------------------------
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
def _load_runtime_config() -> dict:
    """Read INGEST_URL + API_KEY from the APP_CONFIG table (populated by deploy.sh
    from .env). Falls back to env vars for local/dev use."""
    cfg = {}
    try:
        rows = session.sql(
            "SELECT KEY, VALUE FROM SNOWFLAKE_EXAMPLE.CREDIT_DEMO.APP_CONFIG"
        ).collect()
        cfg = {r["KEY"]: r["VALUE"] for r in rows}
    except Exception:
        pass
    tunnel_host = cfg.get("INGEST_TUNNEL_HOST") or os.environ.get(
        "INGEST_TUNNEL_HOST", "<your-tunnel-host>"
    )
    api_key = cfg.get("INGEST_API_KEY") or os.environ.get(
        "INGEST_API_KEY", "<set-via-env-INGEST_API_KEY>"
    )
    return {
        "INGEST_URL": f"https://{tunnel_host}",
        "API_KEY": api_key,
        "TUNNEL_HOST": tunnel_host,
    }


_RUNTIME_CFG = _load_runtime_config()
INGEST_URL = _RUNTIME_CFG["INGEST_URL"]
API_KEY = _RUNTIME_CFG["API_KEY"]
_TUNNEL_HOST = _RUNTIME_CFG["TUNNEL_HOST"]
INT_WH = "CREDIT_DEMO_INT_WH"
STD_WH = "CREDIT_DEMO_WH"
SNOW_BLUE = "#29B5E8"
GREEN = "#21BA45"
RED = "#FF4B4B"
ORANGE = "#FFA500"

# Cortex Agent
AGENT_DB = "SNOWFLAKE_EXAMPLE"
AGENT_SCHEMA = "CREDIT_DEMO"
AGENT_NAME = "CREDIT_AGENT"
SNOWFLAKE_HOST = os.environ.get("SNOWFLAKE_HOST", "")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="ACME Credit Desk",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(f"""
<style>
    div[data-testid="stMetric"] {{
        background: rgba(41, 181, 232, 0.08);
        border: 1px solid rgba(41, 181, 232, 0.2);
        border-radius: 8px;
        padding: 12px 16px;
    }}
    .latency-badge {{
        display: inline-block;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 600;
    }}
    .badge-fast {{ background: rgba(33, 186, 69, 0.15); color: {GREEN}; }}
    .badge-mid  {{ background: rgba(255, 165, 0, 0.15); color: {ORANGE}; }}
    .badge-slow {{ background: rgba(255, 75, 75, 0.15); color: {RED}; }}
    .header-strip {{
        background: linear-gradient(90deg, {SNOW_BLUE} 0%, #1B9CD6 100%);
        padding: 12px 24px;
        border-radius: 8px;
        margin-bottom: 16px;
    }}
    .header-strip h1 {{
        color: white !important;
        font-size: 22px !important;
        margin: 0 !important;
        padding: 0 !important;
    }}
    .header-strip p {{
        color: rgba(255,255,255,0.8);
        font-size: 13px;
        margin: 4px 0 0 0;
    }}
    .latency-panel {{
        background: rgba(41, 181, 232, 0.05);
        border: 1px solid rgba(41, 181, 232, 0.15);
        border-radius: 8px;
        padding: 10px 14px;
        margin-top: 8px;
    }}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="header-strip">
    <h1>ACME Credit Management — Live Credit Desk</h1>
    <p>Real-time trade capture · Snowpipe Streaming HPA · Interactive Table analytics</p>
</div>
""", unsafe_allow_html=True)

# Why-Snowflake / sales narrative — collapsed by default so it doesn't interrupt the demo,
# but available to open mid-pitch when the customer asks "what's this built on?"
with st.expander("🏔️ What this demo proves about Snowflake — click to expand", expanded=False):
    st.markdown("""
This isn't a custom build. **Every layer of this app is a Snowflake-native capability** —
the only "code" is ~500 lines of Python and a 4-channel ingest worker. Here's what your
team gets out-of-the-box:

#### 1. Snowpipe Streaming — High-Performance Architecture (GA Sept 2025)
- **Up to 10 GB/s/table throughput, ingest-to-query as low as 5s** (or **~30ms HPA commit ack** with `wait_for_flush()`, as you're seeing — note the click→tile re-render you're watching is ~3–5s because Streamlit re-runs the whole script on every click; the streaming layer itself commits in tens of ms)
- **Exactly-once delivery** via offset tokens — every event is auditable, no dedupe code on your side
- **Server-side schema validation + in-flight transformations** via the PIPE object — apply `COPY` syntax (filters, casts, MATCH_BY_COLUMN_NAME) at ingest, no separate ETL
- **Auto-PIPE** (`<TABLE>-STREAMING`) is created the moment your producer opens a channel — no DDL ceremony
- **No staging files, no SnowSQL `COPY INTO` orchestration** — rows go directly from your producer into Snowflake

#### 2. Interactive Tables (GA on AWS / Azure / GCP)
- Purpose-built for **sub-second concurrent reads** — the "hot serving" layer your dashboards and Cortex Agents need
- **Cluster keys are mandatory** — Snowflake forces you to think about hot-path access patterns up front
- HPA can write **directly into an Interactive Table** — no intermediate standard table, no 1-minute Dynamic Table refresh hop. *This* is the "single layer" architecture you saw earlier
- Static + dynamic variants — your choice based on whether the IT is the streaming target or a derived rollup

#### 3. Interactive Warehouses
- New compute SKU optimized for **high-concurrency, low-latency analytic workloads**
- **Bound to specific Interactive Tables** via `ALTER WAREHOUSE … ADD TABLES` — keeps query plans cached and result sets warm
- Auto-suspends after 24h idle (vs 1min for standard) — by design, since it's holding hot cache state
- Sub-second SELECTs even when 50 of your analysts hit the same dashboard at the same time

#### 4. Streamlit on Snowflake — Container Runtime
- The UI you're using right now is **running inside Snowflake's compute pool** (`SYSTEM$ST_CONTAINER_RUNTIME_PY3_11`), not on a laptop or AWS EC2
- **External Access Integration (EAI)** lets the app call out to your VM-hosted producer over TLS — no VPC peering, no API gateway
- **`pyproject.toml` + uv** for full Python ecosystem — install plotly, requests, pandas from PyPI without packaging gymnastics
- **No separate auth layer** — Snowsight handles SSO, RBAC, sharing. You add a user to a role; they get the app

#### 5. Cortex Agent — Conversational Analytics (NOW LIVE in the "Ask the Book" tab)
- **Cortex Agent** combines **Cortex Analyst** (text-to-SQL over a Semantic View) and **Cortex Search** (fuzzy issuer name matching) into a single conversational interface
- Ask natural-language questions like "What is today's P&L by sector?" or "Show me Apollo's positions" and get SQL-grounded answers in seconds
- The **Semantic View** maps RAW_EVENTS + POSITIONS_DIM with rich metadata (sample values, column comments, search service bindings) — no model training, no vector DB
- **Cortex Search Service** on POSITIONS_DIM indexes 62 issuers with sub-second fuzzy matching — type "Cascade" and it finds Cascade Industrial, Cascade Mortgage Svcs, Cascade Pipeline Co

#### 6. The producer side (GCP VM, intentionally)
- HPA SDK explicitly **does not run inside SPCS** — Snowflake's design choice. Producers are typically external (Aladdin, Bloomberg, Kafka, custom apps)
- The 4-channel `StreamingService` pattern is the **production-grade reference** from the Cortex `snowpipe-streaming` skill — channel pool, hash-by-key partitioning, self-healing on token rotation, bounded retries
- Cloudflared tunnel = stable public URL for the SiS app to call. Same pattern works for any producer behind a firewall

#### What's NOT in this demo (yet) but ships in the same platform
- **Time Travel + Fail-safe** for SEC recordkeeping retention
- **Replication groups** for BC/DR (Business Critical edition)
- **Snowflake Cortex AI functions** (`AI_CLASSIFY`, `AI_SENTIMENT`, `AI_EXTRACT`) running directly on RAW_EVENTS rows for credit-event classification
- **Native Apps** packaging — the same StreamingService + Interactive Table + Streamlit could be a Snowflake Native App listed on the Marketplace for any asset manager to install

#### The pitch in one sentence
**"You're looking at six Snowflake products working as one platform — Snowpipe Streaming HPA for ingestion, Interactive Tables for hot serving, Interactive Warehouses for sub-second concurrent reads, Streamlit on Snowflake for the UI, Cortex Agent with Analyst + Search for conversational analytics, and Semantic Views for metadata-rich text-to-SQL — none of which require you to operate Kafka, Spark, Redis, a dashboard server, or a vector DB."**
    """)


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "last_fire" not in st.session_state:
    st.session_state.last_fire = None
if "drill_results" not in st.session_state:
    st.session_state.drill_results = []
if "event_history" not in st.session_state:
    # Rolling history of every click for the timeline chart
    st.session_state.event_history = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fire_event(event_type: str, position_id: str | None = None) -> dict:
    """POST to the VM ingest worker and capture per-step latency."""
    body: dict = {"event_type": event_type}
    if position_id:
        body["position_id"] = position_id

    t0 = time.time()
    try:
        r = requests.post(
            f"{INGEST_URL}/ingest",
            headers={
                "Content-Type": "application/json",
                "X-API-Key": API_KEY,
                # Browser-like UA so Cloudflare Bot Fight Mode / BIC doesn't 403 us.
                # Without this, default python-requests UA from Snowflake egress IPs
                # gets flagged as automated traffic. See ASSUMPTIONS.md.
                "User-Agent": f"Mozilla/5.0 (credit-demo SiS Streamlit; +https://{_TUNNEL_HOST})",
            },
            json=body,
            timeout=10,
        )
        r.raise_for_status()
        t1 = time.time()
        resp = r.json()
        roundtrip_ms = (t1 - t0) * 1000
        handler_ms = resp.get("total_handler_ms", 0)
        network_ms = roundtrip_ms - handler_ms

        # Poll Interactive Table until visible
        eid = resp.get("event_id", "")
        visible_ms = None
        if eid:
            poll_start = time.time()
            while time.time() - poll_start < 5:
                try:
                    df = session.sql(
                        f"SELECT 1 FROM SNOWFLAKE_EXAMPLE.CREDIT_DEMO.RAW_EVENTS "
                        f"WHERE EVENT_ID = '{eid}' LIMIT 1"
                    ).to_pandas()
                    if len(df):
                        visible_ms = (time.time() - t0) * 1000
                        break
                except Exception:
                    pass
                time.sleep(0.1)
            if visible_ms is None:
                visible_ms = (time.time() - t0) * 1000

        return {
            "event_id": eid,
            "event_type": event_type,
            "position_id": resp.get("position_id", "?"),
            "partition": resp.get("partition", -1),
            "network_ms": round(network_ms, 1),
            "sdk_appended_ms": resp.get("sdk_appended_ms", 0),
            "flush_committed_ms": resp.get("flush_committed_ms", 0),
            "total_handler_ms": handler_ms,
            "roundtrip_ms": round(roundtrip_ms, 1),
            "visible_ms": round(visible_ms, 1) if visible_ms else None,
            "error": None,
        }
    except Exception as exc:
        t1 = time.time()
        return {
            "event_id": "ERROR",
            "event_type": event_type,
            "position_id": "?",
            "partition": -1,
            "network_ms": 0,
            "sdk_appended_ms": 0,
            "flush_committed_ms": 0,
            "total_handler_ms": 0,
            "roundtrip_ms": round((t1 - t0) * 1000, 1),
            "visible_ms": None,
            "error": str(exc),
        }


def _latency_color(ms: float) -> str:
    if ms < 300:
        return GREEN
    if ms < 1000:
        return ORANGE
    return RED


def _use_interactive_wh():
    try:
        session.sql(f"USE WAREHOUSE {INT_WH}").collect()
    except Exception:
        pass


def _use_standard_wh():
    try:
        session.sql(f"USE WAREHOUSE {STD_WH}").collect()
    except Exception:
        pass


def _get_agent_token() -> str:
    """Get a session token for Cortex Agent REST API calls."""
    try:
        resp = session.connection._rest._token_request("ISSUE")
        return resp["data"]["sessionToken"]
    except Exception:
        return ""


def _read_oauth_token() -> str:
    """Read the SPCS/SiS Container Runtime OAuth token from the canonical path."""
    try:
        from pathlib import Path
        return Path("/snowflake/session/token").read_text().strip()
    except Exception:
        return ""


def _extract_text(payload: dict) -> str:
    """Extract text from any known Cortex Agent SSE payload shape (BDC pattern)."""
    parts: list[str] = []
    # Shape 1: {"delta": {"content": [{"type": "text", "text": "..."}]}}
    delta = payload.get("delta", {})
    content = delta.get("content", [])
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
    elif isinstance(content, dict) and content.get("type") == "text":
        parts.append(content.get("text", ""))
    # Shape 2: {"choices":[{"delta":{"content":"..."}}]}
    for choice in payload.get("choices", []):
        d = choice.get("delta", {})
        c = d.get("content")
        if isinstance(c, str):
            parts.append(c)
    # Shape 3: top-level text
    if "text" in payload and isinstance(payload["text"], str):
        parts.append(payload["text"])
    # Shape 4: top-level content list
    top = payload.get("content", [])
    if isinstance(top, list):
        for item in top:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
    return "".join(parts)


def _stream_agent_response(prompt: str, chat_history: list[dict]) -> str:
    """Call Cortex Agent :run via SSE — BDC-pattern (Bearer OAuth + event-typed parser)."""
    token = _read_oauth_token()
    if not token:
        return ("Error: OAuth token at /snowflake/session/token not available. "
                "This Streamlit must run in SiS Container Runtime (it does — but token may not be mounted).")

    messages = []
    for msg in chat_history:
        messages.append({
            "role": msg["role"],
            "content": [{"type": "text", "text": msg["content"]}],
        })
    messages.append({
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    })

    url = (
        f"https://{SNOWFLAKE_HOST}/api/v2/databases/{AGENT_DB}"
        f"/schemas/{AGENT_SCHEMA}/agents/{AGENT_NAME}:run"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }
    body = {"messages": messages}

    full_text = ""
    try:
        with requests.post(url, headers=headers, json=body, stream=True, timeout=120) as resp:
            if resp.status_code != 200:
                return f"Agent error (HTTP {resp.status_code}): {resp.text[:400]}"

            event_type = None
            data_buf: list[str] = []

            for raw_line in resp.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue
                line = raw_line.strip()

                if line.startswith("event:"):
                    event_type = line.split("event:", 1)[1].strip()
                    data_buf = []
                elif line.startswith("data:"):
                    data_str = line[5:].strip()
                    if data_str == "[DONE]":
                        break
                    data_buf.append(data_str)
                elif line == "" and data_buf:
                    raw = "\n".join(data_buf)
                    data_buf = []
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Skip non-content events
                    if event_type in (
                        "response.thinking.delta", "response.thinking",
                        "response.status",
                        "response.tool_use",
                        "response.tool_result", "response.tool_result.status",
                        "response", "response.text",
                    ):
                        continue
                    if event_type == "error":
                        return f"[Agent error: {payload.get('message', json.dumps(payload)[:300])}]"

                    text = _extract_text(payload)
                    if text:
                        full_text += text
    except requests.exceptions.RequestException as exc:
        return f"Agent error: {exc}"

    return full_text or "_(empty response — agent returned no text content)_"


# ---------------------------------------------------------------------------
# TABS: Live Desk vs Agent Chat
# ---------------------------------------------------------------------------
tab_desk, tab_chat = st.tabs(["Live Credit Desk", "Ask the Book (Cortex Agent)"])

with tab_desk:

    # ===== FULL WIDTH: Latency Timeline (HEADLINE) =====
    st.markdown("### Latency Timeline — Click → Visible in Interactive Table")

    with st.expander("ⓘ How each latency segment is measured", expanded=False):
        st.markdown("""
    The chart breaks **click → visible-in-Interactive-Table** into four real measurements,
    not modeled estimates. Each is captured at a known checkpoint with `time.time()`.

    | # | Segment | Where it's measured | Typical | What you can tune |
    |---|---------|---------------------|---------|-------------------|
    | **1** | **Network**<br/>(client ↔ VM) | Streamlit `t0_click` → POST returns. Subtract VM-internal time. | 50-150ms | Network path only. Cloudflared edge, GCP egress, your ISP. |
    | **2** | **HPA SDK append** | VM FastAPI handler: `t_in_handler` → after `channel.append_row(row)` | **0.1–2ms** | Effectively zero — the SDK buffers locally. |
    | **3** | **HPA flush commit** | VM FastAPI handler: `wait_for_flush(timeout=10)` blocks until Snowflake server confirms commit. | **30-200ms** with `wait_for_flush`<br/>~5-10s WITHOUT it | This is the irreducible Snowflake-side cost. We minimize it via `wait_for_flush()` per row + `max_client_lag=100ms` in the SDK profile. |
    | **4** | **IT poll** | Streamlit re-queries `RAW_EVENTS WHERE event_id = ?` every 100ms until row appears. | 200-500ms | Polling cadence + Interactive Warehouse query latency. |

    **Why it matters for a private credit desk**
    - Without `wait_for_flush()`, post-ack HPA commit lag is 5-10s (the published Snowflake HPA SLA).
    - With `wait_for_flush()`, that drops to ~30ms — a 200x improvement — at the cost of synchronous per-row throughput. Acceptable for click-driven demos; in production you'd batch via `MAX_CLIENT_LAG`.
    - Total **click → row queryable from any other Snowflake connection** is ~150–300 ms — bounded by network and Snowflake's server-side commit, both physically minimal. The Streamlit tile re-render you see in this app is ~3–5 s because the framework re-runs the whole script on every interaction; that's UI-framework cost, not Snowflake.

    **What you're seeing in real time**
    - Bar #1 is usually slowest (cold channel + cold IT cache). Subsequent bars flatten as channels warm.
    - **HPA flush (Snowflake brand blue)** is the dominant cost — that's the conversation: *"this is the actual Snowflake commit, everything else is plumbing."*

    **Architecture in one line**
    `Streamlit (SiS Container Runtime) → cloudflared tunnel → FastAPI on GCP VM → StreamingService (4-channel HPA SDK) → auto-PIPE RAW_EVENTS-STREAMING → RAW_EVENTS Interactive Table → Interactive Warehouse → dashboard tile`

    For the full architecture, latency model, and assumptions, see `ASSUMPTIONS.md` in the project repo.
        """)


    st.caption(
        "**Each bar is one click.** Stacked components show where every millisecond goes "
        "from your click in this app, through the cloudflared tunnel, into the VM\'s "
        "Snowpipe Streaming HPA SDK, and back into the Interactive Table. Snowflake commits "
        "via `wait_for_flush()` in ~30 ms; the row is queryable from any other connection "
        "within a few hundred ms. The dashboard tile you're watching takes ~3–5 s to "
        "re-render because Streamlit re-runs the whole script on every interaction."
    )

    hist = st.session_state.event_history
    if hist:
        rows = []
        for i, h in enumerate(hist, start=1):
            rt = h.get("roundtrip_ms") or 0
            vis = h.get("visible_ms") or 0
            it_poll = max(0, vis - rt) if vis else 0
            label = f"#{i} {h.get('event_type', '?')[:6]}"
            rows.append({"event": label, "step": "1 · Network",       "ms": h.get("network_ms", 0)})
            rows.append({"event": label, "step": "2 · HPA SDK append","ms": h.get("sdk_appended_ms", 0)})
            rows.append({"event": label, "step": "3 · HPA flush",     "ms": h.get("flush_committed_ms", 0)})
            rows.append({"event": label, "step": "4 · IT poll",       "ms": it_poll})
        timeline_df = pd.DataFrame(rows)

        # Dark-mode palette tuned for #0f172a background
        color_map = {
            "1 · Network":        "#67E8F9",   # cyan-300 — client-side network
            "2 · HPA SDK append": "#34D399",   # emerald-400 — instant in-process
            "3 · HPA flush":      "#29B5E8",   # Snowflake brand blue — server-side commit (the headline)
            "4 · IT poll":        "#FBBF24",   # amber-400 — post-commit visibility wait
        }

        fig_tl = px.bar(
            timeline_df, x="event", y="ms", color="step",
            category_orders={
                "event": [f"#{i+1} {h.get('event_type', '?')[:6]}" for i, h in enumerate(hist)],
                "step": list(color_map.keys()),
            },
            color_discrete_map=color_map,
            custom_data=["step"],
        )
        fig_tl.update_traces(
            hovertemplate="<b>%{x}</b><br>%{customdata[0]}: %{y:.1f} ms<extra></extra>",
            marker_line_color="#0f172a", marker_line_width=1,
        )
        fig_tl.update_layout(
            barmode="stack",
            height=300,
            margin=dict(t=44, b=44, l=50, r=10),
            xaxis=dict(
                title=dict(text="Click sequence (oldest → newest)", font=dict(size=11, color="#94a3b8")),
                tickangle=-25, tickfont=dict(size=10, color="#cbd5e1"),
                showgrid=False, zeroline=False, showline=True, linecolor="#334155",
            ),
            yaxis=dict(
                title=dict(text="Latency (ms)", font=dict(size=11, color="#94a3b8")),
                tickfont=dict(size=10, color="#cbd5e1"),
                gridcolor="#1e293b", zeroline=True, zerolinecolor="#334155",
            ),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.04,
                xanchor="left", x=0.0,
                font=dict(size=11, color="#e2e8f0"), title=dict(text=""),
                bgcolor="rgba(0,0,0,0)",
            ),
            plot_bgcolor="#0f172a",
            paper_bgcolor="#0f172a",
            font=dict(color="#e2e8f0"),
            bargap=0.25,
        )
        st.plotly_chart(fig_tl, use_container_width=True, config={"displayModeBar": False})

        # Summary stats below the chart
        completed = [h for h in hist if h.get("visible_ms")]
        if completed:
            totals = sorted([h["visible_ms"] for h in completed])
            flushes = [h.get("flush_committed_ms", 0) for h in completed]
            ts1, ts2, ts3, ts4, ts5 = st.columns(5)
            with ts1: st.metric("Events fired", len(hist))
            with ts2: st.metric("Median click → visible", f"{totals[len(totals)//2]:.0f}ms")
            with ts3: st.metric("Min", f"{min(totals):.0f}ms")
            with ts4: st.metric("Max", f"{max(totals):.0f}ms")
            with ts5: st.metric("Median HPA flush", f"{sorted(flushes)[len(flushes)//2]:.0f}ms")
    else:
        st.info(
            "Fire a few events from the **Generator** below — each click adds a stacked bar "
            "showing the latency breakdown so you can see exactly where time is being spent."
        )

    st.markdown("---")

    # ---------------------------------------------------------------------------
    # LAYOUT: Top row = Generator + wide Live Tape; Below = full-width Dashboard
    # ---------------------------------------------------------------------------
    col_gen, col_tape = st.columns([3, 9])

    # ===== COLUMN 1: Event Generator =====
    with col_gen:
        st.markdown("### Event Generator")

        # Check VM health
        try:
            hc = requests.get(
                f"{INGEST_URL}/health",
                headers={"User-Agent": f"Mozilla/5.0 (credit-demo SiS Streamlit; +https://{_TUNNEL_HOST})"},
                timeout=3,
            )
            hc.raise_for_status()
            health = hc.json()
            ch = health.get("channel_count", 0)
            st.success(f"HPA: {ch} channels · {health.get('pipe_name', '?')}")
        except Exception as exc:
            st.error(f"VM unreachable: {exc}")

        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("Trade", use_container_width=True, type="primary"):
                ev = _fire_event("TRADE")
                st.session_state.last_fire = ev
                if not ev.get("error"):
                    st.session_state.event_history.append({**ev, "ts": time.time()})
        with b2:
            if st.button("Mark", use_container_width=True):
                ev = _fire_event("MARK")
                st.session_state.last_fire = ev
                if not ev.get("error"):
                    st.session_state.event_history.append({**ev, "ts": time.time()})
        with b3:
            if st.button("Credit", use_container_width=True):
                ev = _fire_event("CREDIT_EVENT")
                st.session_state.last_fire = ev
                if not ev.get("error"):
                    st.session_state.event_history.append({**ev, "ts": time.time()})

        # Trim history to last 50 events to keep chart readable
        if len(st.session_state.event_history) > 50:
            st.session_state.event_history = st.session_state.event_history[-50:]

        # Per-step latency panel — compact summary of last event
        ev = st.session_state.last_fire
        if ev and not ev.get("error"):
            st.markdown('<div class="latency-panel">', unsafe_allow_html=True)
            st.caption(f"**{ev['event_type']}** → {ev['position_id']} (P{ev['partition']})")

            rt_ms = ev.get("roundtrip_ms", ev.get("network_ms", 0) + ev.get("total_handler_ms", 0))
            visible_ms = ev.get("visible_ms") or 0
            total_ms = visible_ms or rt_ms
            color = _latency_color(total_ms)

            st.markdown(
                f'Click → visible: '
                f'<span style="font-size:24px;font-weight:700;color:{color}">{total_ms:.0f}ms</span>'
                f' &nbsp; <span style="color:#666;font-size:12px">'
                f'(net {ev.get("network_ms", 0):.0f}ms · sdk {ev.get("sdk_appended_ms", 0):.2f}ms · '
                f'flush {ev.get("flush_committed_ms", 0):.0f}ms)</span>',
                unsafe_allow_html=True,
            )
            st.markdown('</div>', unsafe_allow_html=True)
        elif ev and ev.get("error"):
            st.error(f"Ingest error: {ev['error']}")

        st.divider()

        # Batch / stress
        st.markdown("##### Stress Test")
        stress_n = st.slider("Events", 5, 100, 20, step=5)
        if st.button("Fire Batch", use_container_width=True):
            bar = st.progress(0, text="Firing...")
            results = []
            types = ["TRADE", "MARK", "CREDIT_EVENT"]
            for i in range(stress_n):
                r = _fire_event(types[i % 3])
                results.append(r)
                if not r.get("error"):
                    st.session_state.event_history.append({**r, "ts": time.time()})
                bar.progress((i + 1) / stress_n, text=f"{i+1}/{stress_n}")
            bar.progress(1.0, text="Done!")
            st.session_state.drill_results = results
            # Trim history
            if len(st.session_state.event_history) > 50:
                st.session_state.event_history = st.session_state.event_history[-50:]
            lats = [r["roundtrip_ms"] for r in results if not r.get("error")]
            if lats:
                st.success(f"{len(results)} events · avg {sum(lats)/len(lats):.0f}ms")

        # Show drill results table if present
        if st.session_state.drill_results:
            dr = [r for r in st.session_state.drill_results if not r.get("error")]
            if dr:
                stress_df = pd.DataFrame([{
                    "event": f"#{i+1} {r['event_type'][:4]}",
                    "Network":    round(r.get("network_ms", 0), 1),
                    "SDK append": round(r.get("sdk_appended_ms", 0), 2),
                    "HPA flush":  round(r.get("flush_committed_ms", 0), 1),
                    "IT poll":    round(max(0, (r.get("visible_ms") or 0) - r.get("roundtrip_ms", 0)), 1),
                } for i, r in enumerate(dr)])
                melt = stress_df.melt(id_vars="event", var_name="step", value_name="ms")
                fig2 = px.bar(
                    melt, x="event", y="ms", color="step",
                    color_discrete_map={
                        "Network": "#29B5E8", "SDK append": "#75CFEC",
                        "HPA flush": "#0066B3", "IT poll": "#003C68",
                    },
                )
                fig2.update_layout(
                    barmode="stack", height=260,
                    margin=dict(t=10, b=40, l=40, r=10),
                    xaxis_title="", yaxis_title="ms",
                    legend=dict(orientation="h", yanchor="top", y=-0.15, font=dict(size=10)),
                )
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

    # ===== COLUMN 2: Live Tape =====
    with col_tape:
        st.markdown("### Live Event Tape")

        try:
            t0 = time.time()
            tape_df = session.sql(queries.tape_query(30)).to_pandas()
            tape_ms = round((time.time() - t0) * 1000)
            if not tape_df.empty:
                # Filter warmup rows
                tape_df = tape_df[tape_df.get("SOURCE_APP", pd.Series(dtype=str)).fillna("") != "warmup"]
                st.caption(f"{len(tape_df)} events · query {tape_ms}ms")
                display_cols = [c for c in ["INGESTED_TS", "EVENT_TYPE", "POSITION_ID",
                                "ISSUER", "SECTOR", "SIDE", "QTY", "PRICE",
                                "PREV_MARK", "NEW_MARK", "FROM_RATING", "TO_RATING",
                                "COUNTERPARTY", "LATENCY_MS", "PARTITION"]
                               if c in tape_df.columns]
                st.dataframe(tape_df[display_cols], use_container_width=True,
                             height=620, hide_index=True)
            else:
                st.info("No events yet — click a generator button.")
        except Exception as e:
            st.warning(f"Tape error: {e}")



    # ===== FULL WIDTH: Portfolio Dashboard =====
    st.markdown("---")
    st.markdown("### Portfolio Dashboard")
    st.caption(f"Interactive WH: {INT_WH}")

    _use_interactive_wh()

    # Row 1: KPIs
    try:
        t0 = time.time()
        pnl_df = session.sql(queries.pnl_today()).to_pandas()
        pnl_ms = round((time.time() - t0) * 1000)
        total_pnl = float(pnl_df["TOTAL_PNL"].iloc[0] or 0)
        positions = int(pnl_df["POSITION_COUNT"].iloc[0] or 0)
        gainers = int(pnl_df["GAINERS"].iloc[0] or 0)
        losers = int(pnl_df["LOSERS"].iloc[0] or 0)
    except Exception:
        total_pnl, positions, gainers, losers, pnl_ms = 0, 62, 0, 0, 0

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Today's P&L", f"${total_pnl:,.0f}", delta=f"{gainers}G {losers}L")
    with m2:
        st.metric("Positions", positions)
    with m3:
        try:
            wl_df = session.sql(queries.watchlist()).to_pandas()
            st.metric("Watchlist", len(wl_df))
        except Exception:
            st.metric("Watchlist", "—")
    with m4:
        try:
            lag_df = session.sql(queries.interactive_table_lag()).to_pandas()
            lag_s = int(lag_df["LAG_SECONDS"].iloc[0] or 0)
            st.metric("IT Lag", f"{lag_s}s")
        except Exception:
            st.metric("IT Lag", "—")

    if pnl_ms:
        st.caption(f"KPI query: {pnl_ms}ms via Interactive WH")

    # Row 2: Sector donut + Top marks
    dash_l, dash_r = st.columns(2)

    with dash_l:
        st.markdown("##### Sector Exposure")
        try:
            t0 = time.time()
            sector_df = session.sql(queries.sector_exposure()).to_pandas()
            q_ms = round((time.time() - t0) * 1000)
            if not sector_df.empty:
                fig = px.pie(
                    sector_df, names="SECTOR", values="TOTAL_PAR", hole=0.45,
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_layout(
                    margin=dict(t=10, b=10, l=10, r=10), height=280,
                    showlegend=True, legend=dict(font=dict(size=10)),
                )
                fig.update_traces(textposition="inside", textinfo="label+percent",
                                  textfont_size=10)
                st.plotly_chart(fig, use_container_width=True)
                st.caption(f"Query: {q_ms}ms")
        except Exception as e:
            st.warning(f"Sector chart error: {e}")

    with dash_r:
        st.markdown("##### Top 10 Mark Moves")
        try:
            t0 = time.time()
            marks_df = session.sql(queries.top_marks(10)).to_pandas()
            q_ms = round((time.time() - t0) * 1000)
            if not marks_df.empty:
                st.dataframe(
                    marks_df[["ISSUER", "SECTOR", "CURRENT_MARK", "MARK_CHANGE_BPS", "PNL_TODAY"]],
                    use_container_width=True, height=280, hide_index=True,
                )
                st.caption(f"Query: {q_ms}ms")
        except Exception as e:
            st.warning(f"Top marks error: {e}")

    # Row 3: Watchlist + Hourly trades
    dash_wl, dash_hr = st.columns(2)

    with dash_wl:
        st.markdown("##### Credit Watchlist")
        try:
            t0 = time.time()
            wl_detail = session.sql(queries.watchlist()).to_pandas()
            q_ms = round((time.time() - t0) * 1000)
            if not wl_detail.empty:
                st.dataframe(
                    wl_detail[["ISSUER", "RATING", "PAR_AMOUNT", "PNL_TODAY"]],
                    use_container_width=True, height=200, hide_index=True,
                )
                st.caption(f"Query: {q_ms}ms")
            else:
                st.info("No watchlist positions")
        except Exception as e:
            st.warning(f"Watchlist error: {e}")

    with dash_hr:
        st.markdown("##### Trades per Hour (24h)")
        try:
            t0 = time.time()
            hr_df = session.sql(queries.hourly_trades()).to_pandas()
            q_ms = round((time.time() - t0) * 1000)
            if not hr_df.empty:
                fig = px.bar(hr_df, x="HOUR", y="TRADE_COUNT",
                             color_discrete_sequence=[SNOW_BLUE])
                fig.update_layout(
                    margin=dict(t=10, b=30, l=40, r=10), height=200,
                    xaxis_title="", yaxis_title="Trades", showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)
                st.caption(f"Query: {q_ms}ms")
            else:
                st.info("No trades yet")
        except Exception as e:
            st.warning(f"Hourly trades error: {e}")

    _use_standard_wh()

    # ---------------------------------------------------------------------------
    # Footer: Pipeline Observability
    # ---------------------------------------------------------------------------
    st.divider()
    st.markdown("### Pipeline Observability")
    obs1, obs2, obs3, obs4 = st.columns(4)

    with obs1:
        try:
            lat_df = session.sql(queries.ingest_latency_stats(5)).to_pandas()
            if not lat_df.empty and int(lat_df["EVENT_COUNT"].iloc[0]) > 0:
                st.metric("Ingest p50", f"{int(lat_df['P50_MS'].iloc[0])}ms")
                st.caption(f"p95: {int(lat_df['P95_MS'].iloc[0])}ms · p99: {int(lat_df['P99_MS'].iloc[0])}ms")
            else:
                st.metric("Ingest p50", "—")
        except Exception:
            st.metric("Ingest p50", "—")

    with obs2:
        try:
            lag_df = session.sql(queries.interactive_table_lag()).to_pandas()
            lag_val = int(lag_df["LAG_SECONDS"].iloc[0] or 0)
            st.metric("IT Refresh Lag", f"{lag_val}s")
        except Exception:
            st.metric("IT Refresh Lag", "—")

    with obs3:
        try:
            tp_df = session.sql(queries.throughput(5)).to_pandas()
            epm = float(tp_df["EVENTS_PER_MIN"].iloc[0] or 0)
            st.metric("Throughput", f"{epm:.1f} evt/min")
        except Exception:
            st.metric("Throughput", "—")

    with obs4:
        try:
            cnt_df = session.sql(queries.event_count()).to_pandas()
            st.metric("Total Events (24h)", f"{int(cnt_df['CNT'].iloc[0]):,}")
        except Exception:
            st.metric("Total Events (24h)", "—")


# ---------------------------------------------------------------------------
# TAB 2: Cortex Agent Chat
# ---------------------------------------------------------------------------
with tab_chat:
    st.markdown("### Ask the Book")
    st.caption(
        "Powered by **Cortex Agent** — text-to-SQL via Cortex Analyst + "
        "fuzzy issuer search via Cortex Search. Ask any question about the loan book."
    )

    # Suggested prompts
    SUGGESTED = [
        "What is today's total P&L by sector?",
        "Show the latest 10 trades",
        "Which watchlisted names had a downgrade?",
        "Top 5 mark moves since open",
        "Sector exposure breakdown",
    ]

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Render chat history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Suggested prompt buttons (only show when no history)
    if not st.session_state.chat_history:
        st.markdown("**Try one of these:**")
        cols = st.columns(len(SUGGESTED))
        for i, suggestion in enumerate(SUGGESTED):
            with cols[i]:
                if st.button(suggestion, key=f"suggest_{i}", use_container_width=True):
                    st.session_state._pending_prompt = suggestion
                    st.rerun()

    # Handle pending prompt from button click
    pending = st.session_state.pop("_pending_prompt", None)

    # Chat input
    user_input = st.chat_input("Ask about the loan book…")
    prompt = pending or user_input

    if prompt:
        # Show user message
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Get agent response
        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                response = _stream_agent_response(
                    prompt,
                    st.session_state.chat_history[:-1],  # exclude current prompt (already in messages)
                )
            st.markdown(response)

        st.session_state.chat_history.append({"role": "assistant", "content": response})

    # Clear chat button
    if st.session_state.chat_history:
        if st.button("Clear chat", key="clear_chat"):
            st.session_state.chat_history = []
            st.rerun()

# ---------------------------------------------------------------------------
# Auto-refresh: only when the chat tab hasn't been used yet.
# Once the user starts chatting, disable auto-refresh to preserve state.
# The user can manually refresh the live desk by switching tabs.
# ---------------------------------------------------------------------------
if not st.session_state.get("chat_history"):
    time.sleep(2)
    st.rerun()
