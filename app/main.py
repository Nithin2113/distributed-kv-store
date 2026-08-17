from fastapi import FastAPI, HTTPException
from app.storage import KeyValueStore


app = FastAPI(title="Distributed Key-Value Store")

store = KeyValueStore()


@app.get("/")
def health_check():
    return {"status": "running"}


@app.put("/kv/{key}")
def put_value(key: str, value: str):
    store.put(key, value)

    return {
        "key": key,
        "value": value,
        "status": "stored"
    }


@app.get("/kv/{key}")
def get_value(key: str):
    value = store.get(key)

    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")

    return {
        "key": key,
        "value": value
    }


@app.delete("/kv/{key}")
def delete_value(key: str):
    value = store.delete(key)

    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")

    return {
        "key": key,
        "status": "deleted"
    }