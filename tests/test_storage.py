
from app.storage import KeyValueStore


def test_put_and_get(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    record = store.put("name", "Nithin")

    assert store.get("name") == "Nithin"
    assert record["version"] == 1
    assert record["deleted"] == 0
    assert len(record["checksum"]) == 64

    store.close()


def test_version_increments_on_update(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    first = store.put("name", "Nithin")
    second = store.put("name", "ORBIT")

    assert second["version"] == first["version"] + 1
    assert store.get("name") == "ORBIT"

    store.close()


def test_delete_creates_tombstone(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    store.put("name", "Nithin")
    store.delete("name")

    record = store.get_record("name")

    assert store.get("name") is None
    assert record["deleted"] == 1
    assert record["version"] == 2

    store.close()


def test_missing_key(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    assert store.get("unknown") is None

    store.close()


def test_data_survives_restart(tmp_path):
    db_path = tmp_path / "store.db"

    store = KeyValueStore(db_path)
    store.put("project", "ORBIT DB")
    store.close()

    restarted_store = KeyValueStore(db_path)

    assert restarted_store.get("project") == "ORBIT DB"

    restarted_store.close()


def test_apply_newer_replica_record(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    record = store.put("name", "ORBIT")

    result = store.apply_record(record)

    assert result["success"] is True
    assert store.get("name") == "ORBIT"

    store.close()


def test_reject_older_replica_record(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    old_record = store.put("name", "OLD")
    store.put("name", "NEW")

    result = store.apply_record(old_record)

    assert result["success"] is False
    assert result["reason"] == "stale_version"
    assert store.get("name") == "NEW"

    store.close()


def test_reject_corrupted_replica_record(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    record = {
        "key": "name",
        "value": "CORRUPTED",
        "version": 1,
        "operation_id": "test-operation",
        "updated_at": "2026-11-01T00:00:00+00:00",
        "checksum": "incorrect-checksum",
        "deleted": 0,
    }

    result = store.apply_record(record)

    assert result["success"] is False
    assert result["reason"] == "checksum_mismatch"

    store.close()


def test_reject_same_version_conflict(tmp_path):
    store = KeyValueStore(tmp_path / "store.db")

    record = store.put("name", "ORBIT")

    conflicting_record = dict(record)
    conflicting_record["value"] = "DIFFERENT"
    conflicting_record["checksum"] = store._checksum("DIFFERENT")

    result = store.apply_record(conflicting_record)

    assert result["success"] is False
    assert result["reason"] == "version_conflict"
    assert store.get("name") == "ORBIT"

    store.close()
