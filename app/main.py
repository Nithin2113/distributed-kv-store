import os
from typing import List

from fastapi import FastAPI, HTTPException

from app.node import Node


NODE_ID = os.getenv("NODE_ID", "node-1")
NODE_ROLE = os.getenv("NODE_ROLE", "leader")
NODE_PORT = int(os.getenv("NODE_PORT", "8000"))
STORAGE_PATH = os.getenv("STORAGE_PATH", "data/node1/store.json")
FOLLOWER_URLS: List[str] = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "FOLLOWER_URLS",
        "http://127.0.0.1:8001,http://127.0.0.1:8002",
    ).split(",")
    if url.strip()
]

app = FastAPI(
    title="ORBIT DB",
    description=(
        "A self-healing distributed key-value store with "
        "quorum-based consistency and version-aware replica recovery."
    ),
    version="0.1.0",
)

node = Node(
    node_id=NODE_ID,
    role=NODE_ROLE,
    port=NODE_PORT,
    storage_path=STORAGE_PATH,
    followers=FOLLOWER_URLS if NODE_ROLE == "leader" else [],
)


@app.get("/", tags=["System"])
def root():
    return {
        "service": "ORBIT DB",
        "status": "running",
        "node": node.get_info(),
    }


@app.get("/health/live", tags=["Health"])
def liveness():
    return {"status": "alive", "node_id": node.node_id}


@app.get("/health/ready", tags=["Health"])
def readiness():
    return {
        "status": "ready",
        "node": node.get_info(),
    }


@app.get("/cluster/config", tags=["Cluster"])
def cluster_config():
    return {
        "node": node.get_info(),
        "replication_factor": 1 + len(node.followers) if node.role == "leader" else 1,
        "write_quorum": 2,
        "read_quorum": 2,
        "followers": node.followers,
    }


@app.put("/kv/{key}", tags=["Key-Value"])
def put_value(key: str, value: str):
    node.storage.put(key, value)

    replication = {
        "results": [],
        "successful_nodes": 1,
    }

    if node.role == "leader":
        replication = node.replicate_put(key, value)

    write_quorum = 2
    quorum_achieved = replication["successful_nodes"] >= write_quorum

    response = {
        "key": key,
        "value": value,
        "status": "stored" if quorum_achieved else "quorum_failed",
        "node": node.node_id,
        "write_quorum": write_quorum,
        "successful_nodes": replication["successful_nodes"],
        "quorum_achieved": quorum_achieved,
        "replication": replication["results"],
    }

    if not quorum_achieved:
        raise HTTPException(status_code=503, detail=response)

    return response


@app.get("/kv/{key}", tags=["Key-Value"])
def get_value(key: str):
    value = node.storage.get(key)

    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")

    if node.role != "leader":
        return {
            "key": key,
            "value": value,
            "node": node.node_id,
        }

    read_quorum = 2
    read_results = node.read_from_followers(key)
    quorum_achieved = read_results["successful_nodes"] >= read_quorum

    if not quorum_achieved:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Read quorum not achieved",
                "read_quorum": read_quorum,
                "successful_nodes": read_results["successful_nodes"],
                "replicas": read_results["results"],
            },
        )

    return {
        "key": key,
        "value": value,
        "node": node.node_id,
        "read_quorum": read_quorum,
        "successful_nodes": read_results["successful_nodes"],
        "quorum_achieved": True,
        "replicas": read_results["results"],
    }


@app.delete("/kv/{key}", tags=["Key-Value"])
def delete_value(key: str):
    value = node.storage.delete(key)

    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")

    return {
        "key": key,
        "status": "deleted",
        "node": node.node_id,
    }
