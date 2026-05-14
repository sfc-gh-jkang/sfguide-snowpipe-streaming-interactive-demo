FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency resolution
RUN pip install --no-cache-dir uv

# Install dependencies directly (flat-layout app, no package to build)
COPY pyproject.toml ./
RUN uv pip install --system --no-cache \
    "streamlit>=1.40.0" \
    "snowflake-snowpark-python[pandas]>=1.23.0" \
    "snowflake-connector-python>=3.12.0" \
    "plotly>=5.24.0" \
    "pandas>=2.2.0" \
    "requests>=2.32.0" \
    "opentelemetry-api>=1.27.0" \
    "opentelemetry-sdk>=1.27.0" \
    "opentelemetry-exporter-otlp-proto-http>=1.27.0"

# Copy application code
COPY app.py ingest.py queries.py observability.py ./

ENV STREAMLIT_SERVER_PORT=8080 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8080

CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
