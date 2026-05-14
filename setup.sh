#!/usr/bin/env bash
# setup.sh — Interactive .env generator for the ACME Credit Desk demo.
# Reads .env.example, prompts for each value, writes .env with chmod 600.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_EXAMPLE="${SCRIPT_DIR}/.env.example"
ENV_FILE="${SCRIPT_DIR}/.env"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
bold() { printf '\033[1m%s\033[0m' "$*"; }
dim()  { printf '\033[2m%s\033[0m' "$*"; }
green(){ printf '\033[32m%s\033[0m' "$*"; }

prompt_val() {
    local label="$1" default="$2" secret="${3:-false}" val
    # IMPORTANT: prompt label/UI must go to STDERR — this function is called via
    # $(prompt_val ...) which captures stdout. Only the final value belongs on stdout.
    if [[ "$secret" == "true" ]]; then
        printf "  %-30s " "$(bold "$label"):" >&2
        if [[ -n "$default" && "$default" != "<"* ]]; then
            printf "(press Enter to keep current) " >&2
        fi
        read -rs val
        echo "" >&2  # newline after hidden input
    else
        printf "  %-30s " "$(bold "$label"):" >&2
        if [[ -n "$default" && "$default" != "<"* ]]; then
            printf "[$(dim "$default")] " >&2
        fi
        read -r val
    fi
    echo "${val:-$default}"
}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
echo ""
echo "$(bold '╔══════════════════════════════════════════════════════════╗')"
echo "$(bold '║')  ACME Credit Desk — Interactive Setup                   $(bold '║')"
echo "$(bold '╚══════════════════════════════════════════════════════════╝')"
echo ""
echo "  This script creates $(bold '.env') from $(bold '.env.example')."
echo "  Each value is prompted with the current default (if .env exists)."
echo "  Secrets are entered silently. The file is chmod 600."
echo ""

# Load existing .env as defaults (if present)
# Load existing .env values via grep (bash-3.2 compatible — no associative arrays).
# Why: macOS ships bash 3.2 which doesn't support `declare -A`. This works on any POSIX bash.
_current() {
    [[ -f "$ENV_FILE" ]] || return 0
    local key="$1"
    grep -E "^${key}=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d'=' -f2- || true
}
if [[ -f "$ENV_FILE" ]]; then
    echo "  $(green 'Found existing .env — using current values as defaults.')"
    echo ""
fi

# ---------------------------------------------------------------------------
# 1. Snowflake Connection
# ---------------------------------------------------------------------------
echo "$(bold '── Snowflake Connection ──')"
echo ""

# Try to auto-detect connections
DETECTED_CONN=""
if command -v snow &>/dev/null; then
    echo "  Detected $(bold 'snow') CLI. Available connections:"
    echo ""
    if snow connection list --format json 2>/dev/null | python3 -c "
import sys, json
conns = json.load(sys.stdin)
for i, c in enumerate(conns):
    name = c.get('connection_name', '?')
    acct = c.get('account', '?')
    role = c.get('role', '?')
    print(f'    [{i+1}] {name}  (account={acct}, role={role})')
" 2>/dev/null; then
        echo ""
        printf "  Pick a number, or type a connection name: "
        read -r pick
        if [[ "$pick" =~ ^[0-9]+$ ]]; then
            DETECTED_CONN=$(snow connection list --format json 2>/dev/null | python3 -c "
import sys, json
conns = json.load(sys.stdin)
idx = int('${pick}') - 1
if 0 <= idx < len(conns):
    print(conns[idx].get('connection_name', ''))
" 2>/dev/null || true)
        else
            DETECTED_CONN="$pick"
        fi
    fi
fi

DEFAULT_CONN="${DETECTED_CONN:-$(_current SNOWFLAKE_CONNECTION)}"
SNOWFLAKE_CONNECTION=$(prompt_val "SNOWFLAKE_CONNECTION" "$DEFAULT_CONN")

# Auto-detect account from connection
DEFAULT_ACCT="$(_current SNOWFLAKE_ACCOUNT)"
if [[ -z "$DEFAULT_ACCT" ]] && command -v snow &>/dev/null; then
    DEFAULT_ACCT=$(snow connection list --format json 2>/dev/null | python3 -c "
import sys, json
conns = json.load(sys.stdin)
for c in conns:
    if c.get('connection_name') == '${SNOWFLAKE_CONNECTION}':
        print(c.get('account', ''))
        break
" 2>/dev/null || true)
fi
SNOWFLAKE_ACCOUNT=$(prompt_val "SNOWFLAKE_ACCOUNT" "$DEFAULT_ACCT")
echo ""

# ---------------------------------------------------------------------------
# 2. GCP VM
# ---------------------------------------------------------------------------
echo "$(bold '── GCP VM (Snowpipe Streaming producer) ──')"
echo ""

# Try to auto-detect VMs
if command -v gcloud &>/dev/null; then
    echo "  Detected $(bold 'gcloud'). Listing VMs..."
    gcloud compute instances list --format="table(name,zone,networkInterfaces[0].accessConfigs[0].natIP)" 2>/dev/null | head -10 || true
    echo ""
fi

VM_NAME=$(prompt_val "VM_NAME" "$(_current VM_NAME)")
VM_ZONE=$(prompt_val "VM_ZONE" "$(_current VM_ZONE)")
[[ -z "$VM_ZONE" ]] && VM_ZONE="us-central1-c"
VM_STATIC_IP=$(prompt_val "VM_STATIC_IP" "$(_current VM_STATIC_IP)")
echo ""

# ---------------------------------------------------------------------------
# 3. Cloudflare Tunnel
# ---------------------------------------------------------------------------
echo "$(bold '── Cloudflare Tunnel ──')"
echo ""
INGEST_TUNNEL_HOST=$(prompt_val "INGEST_TUNNEL_HOST" "$(_current INGEST_TUNNEL_HOST)")
echo ""

# ---------------------------------------------------------------------------
# 4. Ingest API Key
# ---------------------------------------------------------------------------
echo "$(bold '── Ingest API Key ──')"
echo ""
echo "  $(dim 'Leave blank to generate a random 32-byte key.')"
INGEST_API_KEY=$(prompt_val "INGEST_API_KEY" "$(_current INGEST_API_KEY)" true)
if [[ -z "$INGEST_API_KEY" || "$INGEST_API_KEY" == "<"* ]]; then
    INGEST_API_KEY=$(openssl rand -base64 32)
    echo "  $(green 'Generated random API key.')"
fi
echo ""

# ---------------------------------------------------------------------------
# Write .env
# ---------------------------------------------------------------------------
cat > "$ENV_FILE" <<ENVEOF
# Auto-generated by setup.sh — $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# NEVER commit this file to git.

# Snowflake
SNOWFLAKE_CONNECTION=${SNOWFLAKE_CONNECTION}
SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}

# GCP VM
VM_NAME=${VM_NAME}
VM_ZONE=${VM_ZONE}
VM_STATIC_IP=${VM_STATIC_IP}

# Cloudflare tunnel
INGEST_TUNNEL_HOST=${INGEST_TUNNEL_HOST}

# Ingest API key
INGEST_API_KEY=${INGEST_API_KEY}
ENVEOF

chmod 600 "$ENV_FILE"

echo "$(bold '── Done ──')"
echo ""
echo "  $(green '✓') Wrote $(bold '.env') (chmod 600)"
echo ""
echo "  Next steps:"
echo "    1. Create Snowflake objects (first time only):"
echo "       $(bold "snow sql -f setup.sql         --enable-templating NONE --connection ${SNOWFLAKE_CONNECTION}")"
echo "       $(bold "snow sql -f semantic_view.sql --enable-templating NONE --connection ${SNOWFLAKE_CONNECTION}")"
echo "       $(dim "(--enable-templating NONE prevents snow CLI's Jinja parser from intercepting '&L' inside P&L instruction text)")"
echo ""
echo "    2. Register the CREDIT_INGEST_USR keypair (one-time)."
echo "       $(dim 'See README step 3.')"
echo ""
echo "    3. Deploy the Streamlit app:"
echo "       $(bold './deploy.sh')"
echo ""
