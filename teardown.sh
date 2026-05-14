#!/usr/bin/env bash
# teardown.sh — Drop ALL demo objects created by setup.sql + deploy.sh.
# Use this to fully reset the demo environment before re-running setup.
# Usage: ./teardown.sh           (interactive — prompts to confirm)
#        ./teardown.sh -y        (skip the prompt)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ ! -f "${SCRIPT_DIR}/.env" ]]; then
    echo "ERROR: .env not found. Copy .env.example to .env and fill in your values." >&2
    exit 1
fi
set -a; source "${SCRIPT_DIR}/.env"; set +a
: "${SNOWFLAKE_CONNECTION:?SNOWFLAKE_CONNECTION must be set in .env}"

DB="SNOWFLAKE_EXAMPLE"
SCHEMA="CREDIT_DEMO"
CONNECTION="${SNOWFLAKE_CONNECTION}"

if [[ "${1:-}" != "-y" ]]; then
    cat <<EOF
This will DROP the following from connection '${CONNECTION}':
  - Streamlit:        ${DB}.${SCHEMA}.CREDIT_LIVE_DESK
  - Agent:            ${DB}.${SCHEMA}.CREDIT_AGENT
  - Cortex Search:    ${DB}.${SCHEMA}.POSITIONS_SEARCH
  - Semantic View:    ${DB}.${SCHEMA}.CREDIT_SV
  - Interactive Tbl:  ${DB}.${SCHEMA}.PORTFOLIO_LIVE
  - Tables:           RAW_EVENTS, POSITIONS_DIM, APP_CONFIG, USER_GRANTS_VIEW
  - Stage:            ${DB}.${SCHEMA}.CREDIT_STAGE
  - Schema:           ${DB}.${SCHEMA}        (CASCADE)
  - EAI:              CREDIT_INGEST_EAI       (account-level)
  - Network Policy:   CREDIT_INGEST_NP        (account-level, per-user override)
  - User:             CREDIT_INGEST_USR       (NETWORK_POLICY unset first)
  - Role:             CREDIT_INGEST_RL
  - Compute Pool:     CREDIT_POOL
  - Warehouses:       CREDIT_DEMO_WH, CREDIT_DEMO_INT_WH

Press Enter to proceed, Ctrl-C to abort.
EOF
    read -r
fi

echo "==> Dropping schema-scoped objects (DROP SCHEMA ... CASCADE handles everything inside)..."
snow sql --connection "${CONNECTION}" -q "
USE ROLE ACCOUNTADMIN;
DROP SCHEMA IF EXISTS ${DB}.${SCHEMA} CASCADE;
"

echo "==> Dropping account-level objects..."
snow sql --connection "${CONNECTION}" -q "
USE ROLE ACCOUNTADMIN;
-- EAI references the network rule (which lived in the dropped schema), so drop EAI first
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS CREDIT_INGEST_EAI;
-- Unset per-user network policy before dropping the policy
ALTER USER IF EXISTS CREDIT_INGEST_USR UNSET NETWORK_POLICY;
DROP NETWORK POLICY IF EXISTS CREDIT_INGEST_NP;
DROP USER IF EXISTS CREDIT_INGEST_USR;
DROP ROLE IF EXISTS CREDIT_INGEST_RL;
DROP COMPUTE POOL IF EXISTS CREDIT_POOL;
DROP WAREHOUSE IF EXISTS CREDIT_DEMO_INT_WH;
DROP WAREHOUSE IF EXISTS CREDIT_DEMO_WH;
"

echo ""
echo "==> Done. Demo objects torn down."
echo ""
echo "To rebuild: ./setup.sh && snow sql -f setup.sql --connection ${CONNECTION} && snow sql -f semantic_view.sql --connection ${CONNECTION} && ./deploy.sh"
