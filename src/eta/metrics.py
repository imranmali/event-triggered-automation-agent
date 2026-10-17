"""Prometheus metrics."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

EVENTS_RECEIVED = Counter(
    "eta_events_received_total",
    "Events received by source, ingress and outcome",
    ["source", "ingress", "outcome"],  # outcome: accepted | duplicate | rejected | no_match
)
RUNS_FINISHED = Counter(
    "eta_runs_finished_total",
    "Run executions by workflow and outcome",
    ["workflow", "outcome"],  # succeeded | retry | dead_lettered | lease_lost | released
)
STEP_DURATION = Histogram(
    "eta_step_duration_seconds",
    "Step execution latency",
    ["workflow", "step", "action", "outcome"],
    buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
RETRIES_SCHEDULED = Counter(
    "eta_retries_scheduled_total", "Retries scheduled", ["workflow", "step"]
)
DEAD_LETTERED = Counter(
    "eta_dead_lettered_total", "Items moved to the dead-letter queue", ["kind", "reason"]
)
LEASES_REAPED = Counter("eta_leases_reaped_total", "Expired leases recovered from lost workers")
INFLIGHT = Gauge("eta_worker_inflight_runs", "Runs currently executing in this process")
QUEUE_READY = Gauge("eta_queue_ready_runs", "Runs ready to execute now")
QUEUE_LAG = Gauge("eta_queue_lag_seconds", "Age of the oldest ready run")
DLQ_OPEN = Gauge("eta_dead_letters_open", "Open dead letters awaiting operator action")
RUNS_BY_STATUS = Gauge("eta_runs", "Runs by status", ["status"])
QUEUE_MESSAGES = Counter(
    "eta_queue_messages_total",
    "Messages consumed from queue ingress",
    ["stream", "outcome"],  # ingested | duplicate | poison | error
)
