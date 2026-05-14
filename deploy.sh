#!/usr/bin/env bash
# deploy.sh — Deploy Streamlit app to Snowflake (Container Runtime)
# Usage: cp .env.example .env && edit .env && ./deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load config from .env (created from .env.example)
if [[ ! -f "${SCRIPT_DIR}/.env" ]]; then
    echo "ERROR: .env not found. Copy .env.example to .env and fill in your values." >&2
    exit 1
fi
set -a; source "${SCRIPT_DIR}/.env"; set +a

: "${SNOWFLAKE_CONNECTION:?SNOWFLAKE_CONNECTION must be set in .env}"

DB="SNOWFLAKE_EXAMPLE"
SCHEMA="CREDIT_DEMO"
STAGE="${DB}.${SCHEMA}.CREDIT_STAGE"
WH="CREDIT_DEMO_WH"
APP_NAME="CREDIT_LIVE_DESK"
POOL="CREDIT_POOL"
CONNECTION="${SNOWFLAKE_CONNECTION}"

: "${INGEST_TUNNEL_HOST:?INGEST_TUNNEL_HOST must be set in .env}"
: "${INGEST_API_KEY:?INGEST_API_KEY must be set in .env}"

# Catch unfilled .env.example placeholders before they MERGE into APP_CONFIG.
case "${INGEST_TUNNEL_HOST}" in
    "<"*)
        echo "ERROR: INGEST_TUNNEL_HOST in .env is still a placeholder ('${INGEST_TUNNEL_HOST}')." >&2
        echo "       Run ./setup.sh, or edit .env directly with your real tunnel hostname." >&2
        exit 1 ;;
esac
case "${INGEST_API_KEY}" in
    "<"*|"set-via-env-INGEST_API_KEY")
        echo "ERROR: INGEST_API_KEY in .env is still a placeholder." >&2
        echo "       Run ./setup.sh (auto-generates a strong random key), or set INGEST_API_KEY=\$(openssl rand -hex 32) in .env." >&2
        exit 1 ;;
esac

echo "==> Populating APP_CONFIG (Streamlit reads runtime config from this table)..."
snow sql --connection "${CONNECTION}" -q "
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ${WH};
MERGE INTO ${DB}.${SCHEMA}.APP_CONFIG t
USING (SELECT 'INGEST_TUNNEL_HOST' AS KEY, '${INGEST_TUNNEL_HOST}' AS VALUE
       UNION ALL SELECT 'INGEST_API_KEY', '${INGEST_API_KEY}') s
ON t.KEY = s.KEY
WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);
"

echo "==> Updating EAI network rule for current tunnel host (${INGEST_TUNNEL_HOST})..."
# Quick-tunnel URLs change on every cloudflared restart — re-run deploy.sh or
# update the rule manually whenever INGEST_TUNNEL_HOST in .env changes.
# trycloudflare.com:443 is also allowed so any quick-tunnel URL works.
snow sql --connection "${CONNECTION}" -q "
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE NETWORK RULE ${DB}.${SCHEMA}.CREDIT_INGEST_RULE
  MODE = EGRESS TYPE = HOST_PORT
  VALUE_LIST = ('${INGEST_TUNNEL_HOST}:443', 'trycloudflare.com:443');
ALTER EXTERNAL ACCESS INTEGRATION CREDIT_INGEST_EAI
  SET ALLOWED_NETWORK_RULES = (${DB}.${SCHEMA}.CREDIT_INGEST_RULE);
"

echo "==> Uploading app files to @${STAGE}..."
for f in app.py ingest.py queries.py observability.py environment.yml pyproject.toml semantic_view.sql; do
    [ -f "${SCRIPT_DIR}/${f}" ] && \
        snow stage copy "${SCRIPT_DIR}/${f}" "@${STAGE}/" --overwrite --connection "${CONNECTION}"
done

echo "==> Recreating Cortex Agent CREDIT_AGENT (idempotent)..."
# Why a Python sub-call instead of 'snow sql -q':
#   1) The agent SPEC contains "P&L" — snow CLI's Jinja templater intercepts &L
#      and aborts with "L is undefined".
#   2) The DDL must be 'CREATE OR REPLACE AGENT ... FROM SPECIFICATION \$\$...\$\$'.
#      The 'SPEC = ''{...}''' form is silently accepted but stores an EMPTY spec,
#      which makes the agent fall through to default tools (no text-to-SQL).
# 'uv run' uses the project's pyproject.toml venv where snowflake-connector-python
# is declared as a dep; system python3 typically lacks the connector.
uv run --with snowflake-connector-python python - <<'PYEOF'
import os, snowflake.connector
conn = snowflake.connector.connect(connection_name=os.environ["SNOWFLAKE_CONNECTION"])
conn.cursor().execute("""
CREATE OR REPLACE AGENT SNOWFLAKE_EXAMPLE.CREDIT_DEMO.CREDIT_AGENT
  WITH PROFILE = '{ "display_name": "Credit Desk Agent" }'
  COMMENT = 'Credit desk analyst — text-to-SQL + fuzzy issuer search'
  FROM SPECIFICATION $$
{
  "models": {"orchestration": "auto"},
  "instructions": {
    "response": "You are a credit-desk analyst assistant for ACME Credit Management. Answer concisely with numbers and tables. When showing P&L, sector exposure, or watchlist data, prefer markdown tables. For event-stream questions (recent trades, marks, downgrades), include event_ts. Always filter out EVENT_TYPE = 'WARMUP' rows unless specifically asked about warmup events.",
    "orchestration": "Use credit_book_analyst for ANY quantitative question (recent trades, P&L, sector breakdowns, top N, watchlist, marks, downgrades, counts, sums). Use issuer_search when the user mentions a specific issuer by partial or fuzzy name. Combine when needed: search to find the issuer name first, then analyst to compute its metrics. Never claim you have no data — always call credit_book_analyst first."
  },
  "tools": [
    {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "credit_book_analyst", "description": "Query RAW_EVENTS (event stream with trades, marks, credit events) and POSITIONS_DIM (62 loan positions with issuer, sector, fund, par amount) for any quantitative question about the credit book."}},
    {"tool_spec": {"type": "cortex_search", "name": "issuer_search", "description": "Find loan positions by fuzzy issuer name match. Returns position_id, sector, tranche, fund, current_rating metadata. Use when a user mentions a company name that might be partial or misspelled."}}
  ],
  "tool_resources": {
    "credit_book_analyst": {
      "execution_environment": {"type": "warehouse", "warehouse": "CREDIT_DEMO_WH"},
      "semantic_view": "SNOWFLAKE_EXAMPLE.CREDIT_DEMO.CREDIT_SV"
    },
    "issuer_search": {
      "id_column": "POSITION_ID",
      "title_column": "ISSUER",
      "max_results": 10,
      "search_service": "SNOWFLAKE_EXAMPLE.CREDIT_DEMO.POSITIONS_SEARCH"
    }
  }
}
$$
""")
print("CREDIT_AGENT recreated.")
PYEOF

echo "==> DROP + CREATE Streamlit (Container Runtime, force fresh container)..."
snow sql --connection "${CONNECTION}" -q "
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ${WH};
DROP STREAMLIT IF EXISTS ${DB}.${SCHEMA}.${APP_NAME};
CREATE STREAMLIT ${DB}.${SCHEMA}.${APP_NAME}
  FROM '@${STAGE}'
  MAIN_FILE = 'app.py'
  RUNTIME_NAME = 'SYSTEM\$ST_CONTAINER_RUNTIME_PY3_11'
  COMPUTE_POOL = ${POOL}
  QUERY_WAREHOUSE = '${WH}'
  EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_ACCESS, CREDIT_INGEST_EAI)
  TITLE = 'ACME — Live Credit Desk';
ALTER STREAMLIT ${DB}.${SCHEMA}.${APP_NAME} ADD LIVE VERSION FROM LAST;
"

echo "==> Done! App URL:"
snow sql --connection "${CONNECTION}" -q "
SELECT CONCAT(
    'https://app.snowflake.com/',
    CURRENT_ORGANIZATION_NAME(), '/',
    CURRENT_ACCOUNT_NAME(),
    '/#/streamlit-apps/${DB}.${SCHEMA}.${APP_NAME}'
) AS APP_URL;
"
