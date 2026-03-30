"""
Health and metrics endpoints.

/health    — basic liveness check (for load balancers)
/metrics   — ingestion pipeline metrics (for dashboards)
"""

from fastapi import APIRouter
from app.core.metrics import ingest_metrics
from app.core.kafka_producer import kafka_producer

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/metrics")
async def metrics():
    """Snapshot of ingestion metrics over the last 60 seconds.

    Example response:
    {
      "window_seconds": 60,
      "messages_received": 142,
      "messages_produced": 140,
      "messages_failed": 2,
      "messages_per_second": 2.37,
      "failure_rate": 0.0141,
      "produce_latency": {
        "count": 140,
        "avg_ms": 4.21,
        "p50_ms": 3.80,
        "p95_ms": 8.12,
        "p99_ms": 12.45,
        "max_ms": 18.90
      },
      "circuit_breaker": {
        "name": "kafka-producer",
        "state": "closed",
        "consecutive_failures": 0,
        "total_rejected": 0
      }
    }
    """
    snapshot = ingest_metrics.snapshot()
    snapshot["circuit_breaker"] = kafka_producer.circuit_stats()
    return snapshot