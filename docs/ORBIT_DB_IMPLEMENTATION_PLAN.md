# ORBIT DB — Implementation Blueprint

## Product goal

Evolve the existing three-node FastAPI key-value store into a Docker-deployed distributed storage platform that demonstrates quorum-based operations, version-aware replication, failure detection, automatic replica repair, integrity verification, observability, and repeatable benchmarks.

This is a research prototype, not a production replacement for DynamoDB, Cassandra, or cloud-managed databases.

## Baseline audit

Inspected on the repository default branch:
- `app/main.py`
- `app/node.py`
- `app/storage.py`

Observed baseline behaviour and gaps:
1. The leader writes locally, then calls follower PUT endpoints. Followers persist locally. The leader counts successful HTTP responses plus itself toward W=2.
2. A write that misses quorum is currently returned with HTTP 200 and status `quorum_failed`; the leader's local write has already been persisted. The API must distinguish committed, rejected, and uncertain outcomes.
3. Leader reads fetch follower values but do not compare versions or select the newest value; the response returns the leader's local value.
4. Follower reads return local data directly, without quorum checks.
5. DELETE changes only the receiving node; leader deletion is not replicated to followers.
6. Storage is a plain JSON dictionary without versions, tombstones, WAL, checksums, or concurrency control.
7. The baseline repository did not contain a dependency manifest, Dockerfile, Compose file, or tracked test suite at the inspected paths.

## Target architecture

- API layer: FastAPI routes, validation, authentication in a later phase.
- Coordinator: operation IDs, quorum policy, commit decision, read reconciliation.
- Storage engine: SQLite-backed records plus append-only write-ahead log and snapshots.
- Replication: versioned mutation envelopes, idempotent follower apply, retryable replication.
- Membership and health: heartbeat loop, node states (HEALTHY, SUSPECT, DOWN, RECOVERING).
- Recovery manager: anti-entropy comparison and incremental repair.
- Integrity: SHA-256 per record; Merkle trees are a stretch goal after basic repair works.
- Observability: Prometheus metrics and a React/TypeScript dashboard using API/WebSocket updates.
- Deployment: Docker Compose with separate containers and persistent volumes for each node.

## Data model

A record should contain:
- key
- value or payload
- version (monotonically increasing per key under the selected single-leader write model)
- operation_id (idempotency)
- updated_at
- checksum
- deleted flag (tombstone)

Replicated deletes must use tombstones. Physical deletion should occur only after a safe retention and replica-acknowledgement policy is defined.

## Consistency contract

Start with a single-leader model and document its limits. Configure N, R, and W; validate 1 <= R,W <= N and R + W > N when quorum intersection is required. This intersection condition alone is not a complete linearizability guarantee.

For the initial versioned protocol:
- Leader assigns a version and operation_id.
- Followers persist a mutation idempotently and acknowledge its version.
- A write is committed only when configured W acknowledgements are reached.
- Reads gather R eligible replica responses and compare versions/checksums.
- Stale replicas are repaired without allowing an older version to overwrite a newer one.
- Timeouts and uncertain outcomes are explicit. A leader crash during an in-flight write requires a documented reconciliation policy.

## Delivery phases and acceptance criteria

### Phase 0 — Reproducible baseline
- Add pinned dependencies, Dockerfile, Compose file, health endpoints, and startup documentation.
- Add tests for PUT/GET/DELETE, replication, unavailable followers, and restart persistence.
- Acceptance: clean Docker Compose startup and baseline tests pass.

### Phase 1 — Correct mutation semantics
- Introduce versioned mutation envelopes and idempotent follower apply.
- Replicate PUT, UPDATE, and DELETE tombstones.
- Return a non-success response when write quorum is not reached; distinguish committed, rejected, and uncertain outcomes.
- Acceptance: tests prove replicas converge for successful operations and failed quorum is not misreported as committed.

### Phase 2 — Version-aware reads and read repair
- Collect R replica responses, compare versions, select the highest valid version under the documented single-leader policy.
- Repair stale replicas asynchronously and safely.
- Acceptance: stale follower is detected and repaired without downgrading newer data.

### Phase 3 — Failure detection and self-healing
- Add heartbeats, timeouts, node state transitions, and recovery registration.
- On restart, compare replica state and transfer missing mutations or a snapshot plus log tail.
- Acceptance: stop a follower, issue committed writes, restart it, observe RECOVERING then HEALTHY, and verify matching versions/checksums.

### Phase 4 — Storage durability and integrity
- Move from JSON dictionary persistence to SQLite records and an append-only WAL.
- Add atomic writes, restart replay, snapshots, and SHA-256 checksums.
- Acceptance: restart tests preserve committed data; injected checksum mismatch is detected and repaired.

### Phase 5 — Docker cluster and management dashboard
- Compose services for leader and followers with independent volumes and internal service DNS.
- React dashboard: topology, node states, quorum settings, per-node versions, replication lag, operation history, recovery progress, and fault-injection controls.
- Acceptance: dashboard reflects actual backend state and controls trigger real test actions.

### Phase 6 — Benchmarks and research evaluation
- Add repeatable workload generator and experiment scripts.
- Measure throughput, p50/p95 latency, write success/commit rate, stale-replica duration, repair time, and recovery bandwidth.
- Compare baseline replication with version-aware repair under identical workloads and failure schedules.
- Acceptance: raw CSV/JSON results, plots, configuration, and reproducible instructions are committed. Report measured values only.

### Stretch goals
- Consistent-hash partitioning and data rebalancing.
- Dynamic node membership.
- Leader election using a real consensus protocol such as Raft (not ad-hoc heartbeat-based promotion).
- Merkle-tree range comparison for efficient anti-entropy.
- JWT/RBAC and audit logging.
- Adaptive repair prioritisation based on measured lag and key importance.

## Suggested repository layout

```
app/
  api/
  cluster/
  consistency/
  replication/
  recovery/
  storage/
  observability/
  main.py
dashboard/
  src/
tests/
  unit/
  integration/
  chaos/
benchmarks/
deploy/
  docker/
docs/
  architecture/
  experiments/
docker-compose.yml
Dockerfile
requirements.txt
```

## Team split

- Engineer A: storage/version model, mutation semantics, quorum reads/writes, read repair.
- Engineer B: Docker Compose, health monitoring, recovery orchestration, dashboard and benchmark harness.
- Both: integration, failure scenarios, code review, and viva preparation.

## Non-negotiable engineering rules

- Keep the original main branch intact; implement on a feature branch.
- Every advertised feature must have an automated test and a demonstrable scenario.
- Do not equate quorum intersection with full consensus or linearizability.
- Do not report a write as committed if W acknowledgements were not obtained.
- Do not claim recovery, security, adaptive routing, or performance improvements until implemented and measured.
