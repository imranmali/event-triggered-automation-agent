# ADR-0002: Leases with heartbeats and fencing tokens

* Status: accepted
* Date: 2026-10-17

## Context

Workers can crash, or stall longer than any timeout (GC pauses, CPU starvation,
network partitions). A crashed worker's runs must be recovered. A *stalled* worker
must never corrupt a run that another worker has since taken over.

## Decision

* A claim sets `lease_owner`, `lease_expires_at` and a random `lease_token`.
* The worker heartbeats every `heartbeat_interval` to extend `lease_expires_at`.
* Every state transition runs `UPDATE runs … WHERE id = :id AND lease_token = :token
  AND status = 'running'` in the same transaction as its other writes. If no row
  matches, it raises `LeaseLostError` and the whole transaction rolls back.
* A reaper on every worker requeues (or dead-letters) runs whose lease expired. Its
  writes add `lease_expires_at < now`, so a heartbeat that just renewed the lease
  wins the race.
* A worker that notices it lost the lease (failed heartbeat fence) cancels its
  in-flight execution.
* Step attempts are counted when the step *starts*, so a crash consumes an attempt
  and a crash loop ends in the DLQ.

## Consequences

* ✅ Zombie writes are impossible, as `test_zombie_worker_cannot_complete_reclaimed_run`
  verifies.
* ✅ Crash recovery needs no coordinator.
* ⚠️ A downstream call made by a zombie *before* it was fenced can't be undone.
  That's why actions receive a stable idempotency key (ADR-0003).
* ⚠️ Correctness assumes bounded clock skew between hosts, so NTP is required.
