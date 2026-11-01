
import hashlib
import os
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.node import Node


NODE_ID = os.getenv("NODE_ID", "node-1")
NODE_ROLE = os.getenv("NODE_ROLE", "leader")
NODE_PORT = int(os.getenv("NODE_PORT", "8000"))
STORAGE_PATH = os.getenv("STORAGE_PATH", "data/node1/orbit.db")

FOLLOWER_URLS: List[str] = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "FOLLOWER_URLS",
        "http://127.0.0.1:8001,http://127.0.0.1:8002",
    ).split(",")
    if url.strip()
]

WRITE_QUORUM = 2
READ_QUORUM = 2


class PutRequest(BaseModel):
    value: str


app = FastAPI(
    title="ORBIT DB",
    description=(
        "A self-healing distributed key-value store with "
        "quorum-based consistency and version-aware replica recovery."
    ),
    version="0.3.0",
)

node = Node(
    node_id=NODE_ID,
    role=NODE_ROLE,
    port=NODE_PORT,
    storage_path=STORAGE_PATH,
    followers=FOLLOWER_URLS if NODE_ROLE == "leader" else [],
)


def validate_record(record):
    """Validate a replica record's structure and checksum."""
    if not isinstance(record, dict):
        return False

    required = {
        "key",
        "value",
        "version",
        "operation_id",
        "updated_at",
        "checksum",
        "deleted",
    }

    if not required.issubset(record):
        return False

    if not isinstance(record["key"], str) or not record["key"]:
        return False

    if type(record["version"]) is not int or record["version"] < 1:
        return False

    if type(record["deleted"]) is not int or record["deleted"] not in (0, 1):
        return False

    if not isinstance(record["operation_id"], str):
        return False

    if not isinstance(record["updated_at"], str):
        return False

    if record["deleted"] == 1:
        if record["value"] is not None:
            return False
        checksum_value = ""
    else:
        if not isinstance(record["value"], str):
            return False
        checksum_value = record["value"]

    expected = hashlib.sha256(
        checksum_value.encode("utf-8")
    ).hexdigest()

    return record["checksum"] == expected


@app.get("/", tags=["System"])
def root():
    return {
        "service": "ORBIT DB",
        "status": "running",
        "node": node.get_info(),
    }


@app.get("/health/live", tags=["Health"])
def liveness():
    return {
        "status": "alive",
        "node_id": node.node_id,
    }


@app.get("/health/ready", tags=["Health"])
def readiness():
    return {
        "status": "ready",
        "node": node.get_info(),
    }


@app.get("/cluster/config", tags=["Cluster"])
def cluster_config():
    replication_factor = (
        1 + len(node.followers) if node.role == "leader" else 1
    )

    return {
        "node": node.get_info(),
        "replication_factor": replication_factor,
        "write_quorum": WRITE_QUORUM,
        "read_quorum": READ_QUORUM,
        "followers": node.followers,
    }


# Internal replication is accepted only by follower nodes.
@app.post("/internal/replicate", tags=["Internal Replication"])
def receive_replica(record: dict):
    if node.role != "follower":
        raise HTTPException(
            status_code=403,
            detail="Internal replication is accepted only by followers",
        )

    result = node.storage.apply_record(record)

    if not result["success"]:
        raise HTTPException(
            status_code=409,
            detail=result,
        )

    return result


# Internal endpoint: return complete record, including tombstones.
@app.get("/internal/record/{key}", tags=["Internal Replication"])
def internal_get_record(key: str):
    return {
        "node": node.node_id,
        "record": node.storage.get_record(key),
    }


# Only the leader can coordinate client writes.
@app.put("/kv/{key}", tags=["Key-Value"])
def put_value(key: str, request: PutRequest):
    if node.role != "leader":
        raise HTTPException(
            status_code=403,
            detail="Client writes are accepted only by the leader",
        )

    value = request.value
    record = node.storage.put(key, value)

    replication = node.replicate_record(record)

    quorum_achieved = (
        replication["successful_nodes"] >= WRITE_QUORUM
    )

    response = {
        "key": key,
        "value": value,
        "version": record["version"],
        "operation_id": record["operation_id"],
        "status": "stored" if quorum_achieved else "quorum_failed",
        "node": node.node_id,
        "write_quorum": WRITE_QUORUM,
        "successful_nodes": replication["successful_nodes"],
        "quorum_achieved": quorum_achieved,
        "replication": replication["results"],
    }

    if not quorum_achieved:
        raise HTTPException(status_code=503, detail=response)

    return response


@app.get("/kv/{key}", tags=["Key-Value"])
def get_value(key: str):
    # Followers can serve reads from their local storage.
    if node.role != "leader":
        value = node.storage.get(key)

        if value is None:
            raise HTTPException(status_code=404, detail="Key not found")

        record = node.storage.get_record(key)

        return {
            "key": key,
            "value": value,
            "version": record["version"],
            "node": node.node_id,
        }

    # Start with the leader's local record.
    local_record = node.storage.get_record(key)
    read_results = node.read_from_followers(key)

    valid_results = []
    invalid_results = []

    for result in read_results["results"]:
        if not result["success"]:
            invalid_results.append(result)
            continue

        follower_record = result.get("record")

        if follower_record is not None:
            if not validate_record(follower_record):
                invalid_results.append({
                    "node": result["node"],
                    "success": False,
                    "reason": "invalid_replica_record",
                })
                continue

            if follower_record["key"] != key:
                invalid_results.append({
                    "node": result["node"],
                    "success": False,
                    "reason": "key_mismatch",
                })
                continue

        valid_results.append(result)

    successful_nodes = 1 + len(valid_results)

    if successful_nodes < READ_QUORUM:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Read quorum not achieved",
                "read_quorum": READ_QUORUM,
                "successful_nodes": successful_nodes,
                "replicas": valid_results + invalid_results,
            },
        )

    # Gather all existing records and choose the highest version.
    candidates = []

    if local_record is not None:
        candidates.append({
            "node": node.node_id,
            "record": local_record,
        })

    for result in valid_results:
        if result.get("record") is not None:
            candidates.append({
                "node": result["node"],
                "record": result["record"],
            })

    if not candidates:
        raise HTTPException(status_code=404, detail="Key not found")

    highest_version = max(
        item["record"]["version"] for item in candidates
    )

    newest_records = [
        item["record"]
        for item in candidates
        if item["record"]["version"] == highest_version
    ]

    # Equal versions must represent the same operation and content.
    reference = newest_records[0]

    for other in newest_records[1:]:
        same_record = (
            reference["operation_id"] == other["operation_id"]
            and reference["checksum"] == other["checksum"]
            and reference["deleted"] == other["deleted"]
            and reference["value"] == other["value"]
        )

        if not same_record:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Replica version conflict",
                    "key": key,
                    "version": highest_version,
                },
            )

    newest_record = reference

    # Repair the leader if it has a stale or missing record.
    if (
        local_record is None
        or local_record["version"] < newest_record["version"]
    ):
        repair_result = node.storage.apply_record(newest_record)

        if not repair_result["success"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Leader read repair failed",
                    "reason": repair_result["reason"],
                },
            )

    # Repair followers that are missing or behind.
    repairs = []

    for result in valid_results:
        follower_record = result.get("record")

        if (
            follower_record is None
            or follower_record["version"] < newest_record["version"]
        ):
            repairs.append(
                node.repair_follower(
                    result["node"],
                    newest_record,
                )
            )

    if newest_record["deleted"]:
        raise HTTPException(status_code=404, detail="Key not found")

    return {
        "key": key,
        "value": newest_record["value"],
        "version": newest_record["version"],
        "operation_id": newest_record["operation_id"],
        "node": node.node_id,
        "read_quorum": READ_QUORUM,
        "successful_nodes": successful_nodes,
        "quorum_achieved": True,
        "replicas": valid_results + invalid_results,
        "repairs": repairs,
    }


# Only the leader can coordinate client deletes.
@app.delete("/kv/{key}", tags=["Key-Value"])
def delete_value(key: str):
    if node.role != "leader":
        raise HTTPException(
            status_code=403,
            detail="Client deletes are accepted only by the leader",
        )

    value = node.storage.delete(key)

    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")

    tombstone = node.storage.get_record(key)
    replication = node.replicate_record(tombstone)

    quorum_achieved = (
        replication["successful_nodes"] >= WRITE_QUORUM
    )

    response = {
        "key": key,
        "status": "deleted" if quorum_achieved else "quorum_failed",
        "version": tombstone["version"],
        "node": node.node_id,
        "write_quorum": WRITE_QUORUM,
        "successful_nodes": replication["successful_nodes"],
        "quorum_achieved": quorum_achieved,
        "replication": replication["results"],
    }

    if not quorum_achieved:
        raise HTTPException(status_code=503, detail=response)

    return response
