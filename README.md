# ORBIT DB

**Design and Evaluation of a Self-Healing Distributed Key-Value Store with Version-Aware Replica Recovery**

ORBIT DB is an educational distributed-storage research platform. It evolves a three-node leader–follower key-value store into a system for studying quorum-based operations, replica consistency, failure detection, automated recovery, and performance under controlled failures.

> Status: foundation phase. Version-aware recovery, durable commit semantics, monitoring, and the management dashboard are planned milestones, not currently completed features.

## Current baseline

- Python and FastAPI service.
- Leader–follower topology.
- Local persistent key-value storage.
- HTTP replication to followers.
- Configured read and write quorum of two in the initial three-node setup.

Known baseline limitations addressed by the roadmap include mutation versioning, replicated deletes, explicit commit/uncertain-write semantics, version-aware reads, recovery after missed writes, and reproducible Docker deployment.

## Start the three-node cluster

Requirements: Docker Engine and Docker Compose plugin.

```bash
docker compose up --build
```

Services:
- Leader API: http://localhost:8000
- Follower 1: http://localhost:8001
- Follower 2: http://localhost:8002
- OpenAPI documentation: http://localhost:8000/docs

Stop the cluster:

```bash
docker compose down
```

Each node has its own persistent Docker volume. To intentionally remove all cluster data, use `docker compose down -v`.

## Basic API examples

```bash
# Write
curl -X PUT "http://localhost:8000/kv/example?value=hello"

# Read
curl "http://localhost:8000/kv/example"

# Delete
curl -X DELETE "http://localhost:8000/kv/example"

# Health
curl "http://localhost:8000/health/ready"
```

## Engineering roadmap

1. Reproducible Docker-based three-node baseline.
2. Versioned mutations, idempotent replication, and explicit quorum commit outcomes.
3. Version-aware reads, conflict detection, and read repair.
4. Heartbeat-based failure detection and automatic replica recovery.
5. Durable storage, write-ahead logging, snapshots, and integrity checks.
6. Professional management dashboard and observability.
7. Fault-injection experiments and reproducible performance evaluation.

See [docs/ORBIT_DB_IMPLEMENTATION_PLAN.md](docs/ORBIT_DB_IMPLEMENTATION_PLAN.md) for architecture, acceptance criteria, and research evaluation.

## Consistency note

The initial configuration uses N=3, R=2, and W=2. The intersection condition R+W>N is necessary for quorum overlap, but does not by itself establish linearizability or consensus. ORBIT DB will document and test the guarantees of its actual implementation; it does not claim to implement Raft or production-grade consensus.

## Development

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```
