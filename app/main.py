import os

from fastapi import FastAPI, HTTPException

from app.node import Node


NODE_ID = os.getenv("NODE_ID", "node-1")
NODE_ROLE = os.getenv("NODE_ROLE", "leader")
NODE_PORT = int(os.getenv("NODE_PORT", "8000"))
STORAGE_PATH = os.getenv("STORAGE_PATH", "data/node1/store.json")


app = FastAPI(title="Distributed Key-Value Store")


node = Node(
    node_id=NODE_ID,
    role=NODE_ROLE,
    port=NODE_PORT,
    storage_path=STORAGE_PATH,
    followers=[
        "http://127.0.0.1:8001",
        "http://127.0.0.1:8002",
    ] if NODE_ROLE == "leader" else [],
)


@app.get("/")
def health_check():
    return {
        "status": "running",
        "node": node.get_info(),
    }


@app.put("/kv/{key}")
def put_value(key: str, value: str):
    node.storage.put(key, value)

    replication = {
        "results": [],
        "successful_nodes": 1,
    }

    if node.role == "leader":
        replication = node.replicate_put(key, value)

    write_quorum = 2

    quorum_achieved = (
        replication["successful_nodes"] >= write_quorum
    )

    return {
        "key": key,
        "value": value,
        "status": "stored" if quorum_achieved else "quorum_failed",
        "node": node.node_id,
        "write_quorum": write_quorum,
        "successful_nodes": replication["successful_nodes"],
        "quorum_achieved": quorum_achieved,
        "replication": replication["results"],
    }


@app.get("/kv/{key}")
def get_value(key: str):
    value = node.storage.get(key)

    if value is None:
        raise HTTPException(
            status_code=404,
            detail="Key not found",
        )

    # Followers return their local value directly.
    if node.role != "leader":
        return {
            "key": key,
            "value": value,
            "node": node.node_id,
        }

    read_quorum = 2

    read_results = node.read_from_followers(key)

    quorum_achieved = (
        read_results["successful_nodes"] >= read_quorum
    )

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


@app.delete("/kv/{key}")
def delete_value(key: str):
    value = node.storage.delete(key)

    if value is None:
        raise HTTPException(
            status_code=404,
            detail="Key not found",
        )

    return {
        "key": key,
        "status": "deleted",
        "node": node.node_id,
    }