"""Parameterized SQL queries for the ACME Credit demo."""

SCHEMA = "SNOWFLAKE_EXAMPLE.CREDIT_DEMO"


def tape_query(limit: int = 30) -> str:
    """Last N raw events joined with issuer/sector for the live tape."""
    return f"""
    SELECT
        e.INGESTED_TS,
        e.EVENT_TYPE,
        e.POSITION_ID,
        p.ISSUER,
        p.SECTOR,
        TIMESTAMPDIFF('millisecond', e.EVENT_TS, e.INGESTED_TS) AS LATENCY_MS,
        COALESCE(e.SIDE, '') AS SIDE,
        COALESCE(e.QTY, 0)  AS QTY,
        e.PRICE,
        e.PREV_MARK,
        e.NEW_MARK,
        COALESCE(e.FROM_RATING, '') AS FROM_RATING,
        COALESCE(e.TO_RATING, '') AS TO_RATING,
        COALESCE(e.COUNTERPARTY, '') AS COUNTERPARTY,
        COALESCE(e.SOURCE_APP, '') AS SOURCE_APP
    FROM {SCHEMA}.RAW_EVENTS e
    LEFT JOIN {SCHEMA}.POSITIONS_DIM p USING (POSITION_ID)
    WHERE COALESCE(e.SOURCE_APP, '') != 'warmup'
    ORDER BY e.EVENT_TS DESC
    LIMIT {limit}
    """


def pnl_today() -> str:
    """Total P&L across all positions from the Interactive Table."""
    return f"""
    SELECT
        ROUND(SUM(PNL_TODAY), 2) AS TOTAL_PNL,
        COUNT(*) AS POSITION_COUNT,
        SUM(CASE WHEN PNL_TODAY > 0 THEN 1 ELSE 0 END) AS GAINERS,
        SUM(CASE WHEN PNL_TODAY < 0 THEN 1 ELSE 0 END) AS LOSERS
    FROM {SCHEMA}.PORTFOLIO_LIVE_VIEW
    """


def sector_exposure() -> str:
    """Par-weighted sector exposure for donut chart."""
    return f"""
    SELECT
        SECTOR,
        ROUND(SUM(PAR_AMOUNT), 0) AS TOTAL_PAR,
        ROUND(SUM(PAR_AMOUNT) / NULLIF(SUM(SUM(PAR_AMOUNT)) OVER (), 0) * 100, 1)
            AS PCT
    FROM {SCHEMA}.PORTFOLIO_LIVE_VIEW
    GROUP BY SECTOR
    ORDER BY TOTAL_PAR DESC
    """


def top_marks(n: int = 10) -> str:
    """Top N positions by absolute mark change."""
    return f"""
    SELECT
        POSITION_ID,
        ISSUER,
        SECTOR,
        TRANCHE,
        CURRENT_MARK,
        ROUND(MARK_CHANGE_BPS, 1) AS MARK_CHANGE_BPS,
        ROUND(PNL_TODAY, 0) AS PNL_TODAY,
        FUND
    FROM {SCHEMA}.PORTFOLIO_LIVE_VIEW
    ORDER BY ABS(MARK_CHANGE_BPS) DESC
    LIMIT {n}
    """


def watchlist() -> str:
    """Credit watchlist positions."""
    return f"""
    SELECT
        POSITION_ID,
        ISSUER,
        RATING,
        SECTOR,
        ROUND(PAR_AMOUNT, 0) AS PAR_AMOUNT,
        CURRENT_MARK,
        ROUND(PNL_TODAY, 0) AS PNL_TODAY
    FROM {SCHEMA}.PORTFOLIO_LIVE_VIEW
    WHERE WATCHLIST = TRUE
    ORDER BY PNL_TODAY ASC
    """


def hourly_trades() -> str:
    """Trade event count by hour for bar chart."""
    return f"""
    SELECT
        DATE_TRUNC('hour', EVENT_TS) AS HOUR,
        COUNT(*) AS TRADE_COUNT
    FROM {SCHEMA}.RAW_EVENTS
    WHERE EVENT_TYPE = 'TRADE'
      AND EVENT_TS >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    GROUP BY 1
    ORDER BY 1
    """


def event_count() -> str:
    """Total event count in the last 24h."""
    return f"""
    SELECT COUNT(*) AS CNT
    FROM {SCHEMA}.RAW_EVENTS
    WHERE EVENT_TS >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    """


def ingest_latency_stats(window_min: int = 5) -> str:
    """Ingest latency percentiles over the last N minutes."""
    return f"""
    SELECT
        COUNT(*) AS EVENT_COUNT,
        ROUND(APPROX_PERCENTILE(
            TIMESTAMPDIFF('millisecond', EVENT_TS, INGESTED_TS), 0.5
        ), 0) AS P50_MS,
        ROUND(APPROX_PERCENTILE(
            TIMESTAMPDIFF('millisecond', EVENT_TS, INGESTED_TS), 0.95
        ), 0) AS P95_MS,
        ROUND(APPROX_PERCENTILE(
            TIMESTAMPDIFF('millisecond', EVENT_TS, INGESTED_TS), 0.99
        ), 0) AS P99_MS
    FROM {SCHEMA}.RAW_EVENTS
    WHERE EVENT_TS >= DATEADD('minute', -{window_min}, CURRENT_TIMESTAMP())
    """


def interactive_table_lag() -> str:
    """Lag between now and the latest event reflected in PORTFOLIO_LIVE."""
    return f"""
    SELECT
        TIMESTAMPDIFF('second', MAX(LATEST_EVENT_TS), CURRENT_TIMESTAMP()) AS LAG_SECONDS
    FROM {SCHEMA}.PORTFOLIO_LIVE_VIEW
    """


def throughput(window_min: int = 5) -> str:
    """Events per minute over the last N minutes."""
    return f"""
    SELECT
        ROUND(COUNT(*) / GREATEST({window_min}, 1), 1) AS EVENTS_PER_MIN
    FROM {SCHEMA}.RAW_EVENTS
    WHERE EVENT_TS >= DATEADD('minute', -{window_min}, CURRENT_TIMESTAMP())
    """
