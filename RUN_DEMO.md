# Live Credit Desk Demo — Runbook

## Pre-flight (5 min before demo)

### 1. Open the SSH tunnel to the VM ingest worker

In a **separate terminal** (or tmux pane):

```bash
gcloud compute ssh <your-vm-name> \
  --zone us-central1-c \
  --tunnel-through-iap \
  -- -L 8080:localhost:8080 -N
```

Leave this running for the duration of the demo.

### 2. Verify the tunnel

```bash
curl http://localhost:8080/health
```

Expected response (4 channels, status ok):
```json
{"status": "ok", "channel_count": 4, "partition_count": 4, ...}
```

If `channel_count: 0` or connection refused, the VM container may need a restart:
```bash
gcloud compute ssh <your-vm-name> --zone us-central1-c --tunnel-through-iap \
  -- 'cd /opt/credit-ingest && docker compose restart ingest-worker'
```
Wait 45-60s for channels to initialize, then re-check health.

### 3. Start Streamlit

```bash
cd ~/Documents/vscode/credit-demo
SNOWFLAKE_CONNECTION_NAME=<your-connection> \
  CREDIT_INGEST_URL=http://localhost:8080 \
  INGEST_API_KEY=<set-via-env-INGEST_API_KEY> \
  uv run streamlit run app.py
```

Open browser at **http://localhost:8501**.

### 4. Pre-warm the pipeline

- Click **New Trade** 3x — verify events appear in the Live Event Tape
- Check the sidebar shows "VM worker: 4 channels active"
- Confirm the partition badge (P0–P3) appears next to each event
- Verify dashboard tiles populate (sector donut, marks table, KPIs)

### 5. Optional: stress test

- Set slider to 50, click **Fire Batch**
- Watch partition distribution in the caption below the button
- Confirm avg latency < 200ms for HPA mode

---

## During the demo

### Talking points

1. **Ingest mode toggle** (sidebar) — switch between Snowpipe Streaming HPA
   and direct INSERT to show the latency difference
2. **Partition assignment** — each event shows P0–P3 badge, demonstrating
   the 4-channel StreamingService pool routing by position_id hash
3. **Server vs roundtrip latency** — badge shows total roundtrip; the
   `server: Xms` annotation shows SDK-level ingest time (typically <20ms)
4. **Live Tape** — queries RAW_EVENTS via standard WH; shows sub-second
   commit visibility from Snowpipe Streaming
5. **Dashboard tiles** — queries PORTFOLIO_LIVE Interactive Table via
   CREDIT_DEMO_INT_WH (sub-second query time, ~1min data freshness via
   TARGET_LAG)
6. **IT Lag metric** — shows seconds between latest event and Interactive
   Table refresh, demonstrating the TARGET_LAG pipeline

### If the tunnel drops

The app auto-detects tunnel failure. The sidebar will show "VM worker
unreachable" and fall back to INSERT mode. Re-establish the tunnel and
the next health check (on page refresh) will restore HPA mode.

---

## Teardown

1. Stop Streamlit: Ctrl+C in the Streamlit terminal
2. Close the SSH tunnel: Ctrl+C in the tunnel terminal
3. VM containers keep running — no action needed
