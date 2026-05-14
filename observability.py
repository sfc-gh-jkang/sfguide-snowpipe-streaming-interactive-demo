"""Observability helpers for ACME demo.

Provides lightweight metric recording. Falls back to no-op
if external endpoints are not configured.
"""

import time
from collections import defaultdict

# In-memory metrics store (reset on container restart)
_metrics: dict[str, list[tuple[float, float]]] = defaultdict(list)
_MAX_POINTS = 1000


def record(metric_name: str, value: float, labels: dict | None = None):
    """Record a metric data point with timestamp."""
    key = metric_name
    if labels:
        suffix = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        key = f"{metric_name}{{{suffix}}}"
    _metrics[key].append((time.time(), value))
    # Trim
    if len(_metrics[key]) > _MAX_POINTS:
        _metrics[key] = _metrics[key][-_MAX_POINTS:]


def get_recent(metric_name: str, window_seconds: int = 300) -> list[tuple[float, float]]:
    """Get recent data points for a metric within the time window."""
    cutoff = time.time() - window_seconds
    return [(ts, val) for ts, val in _metrics.get(metric_name, []) if ts >= cutoff]


def summary(metric_name: str, window_seconds: int = 300) -> dict:
    """Get summary stats for a metric."""
    points = get_recent(metric_name, window_seconds)
    if not points:
        return {"count": 0, "p50": 0, "p95": 0, "avg": 0, "max": 0}
    vals = sorted(v for _, v in points)
    n = len(vals)
    return {
        "count": n,
        "p50": vals[n // 2],
        "p95": vals[int(n * 0.95)] if n >= 20 else vals[-1],
        "avg": round(sum(vals) / n, 1),
        "max": vals[-1],
    }
