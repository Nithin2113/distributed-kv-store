from app.storage import KeyValueStore


def test_put_and_get(tmp_path):
    store = KeyValueStore(tmp_path / "store.json")

    store.put("name", "Nithin")

    assert store.get("name") == "Nithin"


def test_delete(tmp_path):
    store = KeyValueStore(tmp_path / "store.json")

    store.put("name", "Nithin")
    store.delete("name")

    assert store.get("name") is None


def test_missing_key(tmp_path):
    store = KeyValueStore(tmp_path / "store.json")

    assert store.get("unknown") is None