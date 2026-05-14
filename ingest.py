"""Event generator for the ACME Credit demo.

Two ingest modes:
  1. HPA (Snowpipe Streaming via VM worker) — POST to VM /ingest endpoint
  2. INSERT fallback — direct session.sql() INSERT into RAW_EVENTS
"""

import os
import random
import time
import uuid
from datetime import datetime

import requests as _requests

# OTel tracing — optional, no-op if not configured
try:
    from opentelemetry import trace
    _tracer = trace.get_tracer(__name__)
except ImportError:
    _tracer = None

_COUNTERPARTIES = [
    "Goldman Sachs", "JP Morgan", "Morgan Stanley", "Barclays",
    "Citi", "BofA Securities", "Jefferies", "RBC Capital",
    "Nomura", "Deutsche Bank", "Wells Fargo", "HSBC",
]

_MARK_SOURCES = [
    "Broker Desk", "Internal Valuation", "Markit", "Bloomberg BVAL",
    "ICE Data", "Refinitiv", "Third-Party Pricing", "Agent Bank",
]

_AGENCIES = ["S&P", "Moody's", "Fitch", "KBRA", "DBRS"]

_RATINGS = [
    "AAA", "AA+", "AA", "AA-", "A+", "A", "A-",
    "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-",
    "B+", "B", "B-", "CCC+", "CCC", "CCC-", "CC", "C", "D", "NR",
]


def _random_position(session) -> dict:
    """Pick a random position from POSITIONS_DIM."""
    rows = session.sql(
        "SELECT POSITION_ID, ISSUER, BASELINE_MARK, CURRENT_RATING "
        "FROM SNOWFLAKE_EXAMPLE.CREDIT_DEMO.POSITIONS_DIM "
        "ORDER BY RANDOM() LIMIT 1"
    ).collect()
    if not rows:
        return {
            "position_id": "POS-0001",
            "issuer": "Unknown",
            "baseline_mark": 100.0,
            "current_rating": "B",
        }
    r = rows[0]
    return {
        "position_id": r["POSITION_ID"],
        "issuer": r["ISSUER"],
        "baseline_mark": float(r["BASELINE_MARK"]),
        "current_rating": r["CURRENT_RATING"],
    }


def _gen_trade(pos: dict) -> dict:
    """Generate a TRADE event payload."""
    side = random.choice(["BUY", "SELL"])
    qty = round(random.uniform(100_000, 5_000_000), 2)
    price = round(pos["baseline_mark"] + random.uniform(-2, 2), 4)
    return {
        "event_type": "TRADE",
        "side": side,
        "qty": qty,
        "price": price,
        "counterparty": random.choice(_COUNTERPARTIES),
        "prev_mark": None,
        "new_mark": None,
        "mark_source": None,
        "from_rating": None,
        "to_rating": None,
        "agency": None,
    }


def _gen_mark(pos: dict) -> dict:
    """Generate a MARK event payload."""
    prev = pos["baseline_mark"]
    delta = random.uniform(-3, 3)
    new = round(prev + delta, 4)
    return {
        "event_type": "MARK",
        "side": None,
        "qty": None,
        "price": None,
        "counterparty": None,
        "prev_mark": round(prev, 4),
        "new_mark": new,
        "mark_source": random.choice(_MARK_SOURCES),
        "from_rating": None,
        "to_rating": None,
        "agency": None,
    }


def _gen_credit_event(pos: dict) -> dict:
    """Generate a CREDIT_EVENT payload."""
    from_r = pos["current_rating"]
    idx = _RATINGS.index(from_r) if from_r in _RATINGS else 10
    direction = random.choice([-1, -1, -1, 1])  # bias toward downgrades
    to_idx = max(0, min(len(_RATINGS) - 1, idx + direction))
    return {
        "event_type": "CREDIT_EVENT",
        "side": None,
        "qty": None,
        "price": None,
        "counterparty": None,
        "prev_mark": None,
        "new_mark": None,
        "mark_source": None,
        "from_rating": from_r,
        "to_rating": _RATINGS[to_idx],
        "agency": random.choice(_AGENCIES),
    }


_GENERATORS = {
    "TRADE": _gen_trade,
    "MARK": _gen_mark,
    "CREDIT_EVENT": _gen_credit_event,
}


def fire_event(session, event_type: str) -> dict:
    """Insert one event into RAW_EVENTS and return timing info.

    Args:
        session: Snowpark session (from get_active_session)
        event_type: One of TRADE, MARK, CREDIT_EVENT

    Returns:
        dict with event_id, latency_ms, position_id, event_type
    """
    pos = _random_position(session)
    gen = _GENERATORS[event_type]
    payload = gen(pos)

    event_id = str(uuid.uuid4())[:12].upper()
    event_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    # Build parameterized INSERT
    sql = """
    INSERT INTO SNOWFLAKE_EXAMPLE.CREDIT_DEMO.RAW_EVENTS
        (EVENT_ID, EVENT_TS, EVENT_TYPE, POSITION_ID,
         SIDE, QTY, PRICE, COUNTERPARTY,
         PREV_MARK, NEW_MARK, MARK_SOURCE,
         FROM_RATING, TO_RATING, AGENCY, SOURCE_APP)
    SELECT
        '{event_id}',
        '{event_ts}'::TIMESTAMP_NTZ,
        '{event_type}',
        '{position_id}',
        {side},
        {qty},
        {price},
        {counterparty},
        {prev_mark},
        {new_mark},
        {mark_source},
        {from_rating},
        {to_rating},
        {agency},
        'streamlit_demo'
    """.format(
        event_id=event_id,
        event_ts=event_ts,
        event_type=payload["event_type"],
        position_id=pos["position_id"],
        side=_sql_str(payload["side"]),
        qty=_sql_num(payload["qty"]),
        price=_sql_num(payload["price"]),
        counterparty=_sql_str(payload["counterparty"]),
        prev_mark=_sql_num(payload["prev_mark"]),
        new_mark=_sql_num(payload["new_mark"]),
        mark_source=_sql_str(payload["mark_source"]),
        from_rating=_sql_str(payload["from_rating"]),
        to_rating=_sql_str(payload["to_rating"]),
        agency=_sql_str(payload["agency"]),
    )

    t0 = time.time()
    span_ctx = _tracer.start_as_current_span("fire_event") if _tracer else None
    try:
        if span_ctx:
            span = span_ctx.__enter__()
            span.set_attribute("event.type", event_type)
            span.set_attribute("position.id", pos["position_id"])
            span.set_attribute("event.id", event_id)
        session.sql(sql).collect()
        latency_ms = round((time.time() - t0) * 1000)
        if span_ctx and span:
            span.set_attribute("ingest.latency_ms", latency_ms)
    finally:
        if span_ctx:
            span_ctx.__exit__(None, None, None)

    return {
        "event_id": event_id,
        "latency_ms": latency_ms,
        "position_id": pos["position_id"],
        "issuer": pos["issuer"],
        "event_type": event_type,
    }


def fire_batch(session, count: int = 10, event_types=None) -> list[dict]:
    """Fire a batch of events. Returns list of results."""
    if event_types is None:
        event_types = ["TRADE", "MARK", "CREDIT_EVENT"]
    results = []
    for _ in range(count):
        et = random.choice(event_types)
        results.append(fire_event(session, et))
    return results


def fire_event_hpa(event_type: str, position_id: str | None = None) -> dict:
    """Fire one event via the VM ingest worker (Snowpipe Streaming HPA).

    Args:
        event_type: One of TRADE, MARK, CREDIT_EVENT
        position_id: Optional position ID override (random if None)

    Returns:
        dict with event_id, ingested_ms, partition, position_id, event_type
    """
    url = os.environ.get("CREDIT_INGEST_URL", "http://localhost:8080")
    api_key = os.environ.get("INGEST_API_KEY", "")
    body: dict = {"event_type": event_type}
    if position_id:
        body["position_id"] = position_id

    headers: dict = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    t0 = time.time()
    try:
        resp = _requests.post(
            f"{url}/ingest",
            json=body,
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        roundtrip_ms = round((time.time() - t0) * 1000)
        return {
            "event_id": data.get("event_id", "?"),
            "latency_ms": roundtrip_ms,
            "ingested_ms": data.get("ingested_ms", 0),
            "partition": data.get("partition", -1),
            "position_id": data.get("position_id", "?"),
            "event_type": event_type,
            "issuer": data.get("position_id", "?"),  # VM doesn't return issuer
            "mode": "HPA",
        }
    except Exception as exc:
        roundtrip_ms = round((time.time() - t0) * 1000)
        return {
            "event_id": "ERROR",
            "latency_ms": roundtrip_ms,
            "ingested_ms": 0,
            "partition": -1,
            "position_id": position_id or "?",
            "event_type": event_type,
            "issuer": "ERROR",
            "mode": "HPA",
            "error": str(exc),
        }


def check_hpa_health() -> dict | None:
    """Check VM ingest worker health. Returns status dict or None on failure."""
    url = os.environ.get("CREDIT_INGEST_URL", "http://localhost:8080")
    try:
        resp = _requests.get(f"{url}/health", timeout=3)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _sql_str(val) -> str:
    """Wrap value in SQL single quotes, or return NULL."""
    if val is None:
        return "NULL"
    return "'{}'".format(str(val).replace("'", "''"))


def _sql_num(val) -> str:
    """Return numeric literal or NULL."""
    if val is None:
        return "NULL"
    return str(val)
