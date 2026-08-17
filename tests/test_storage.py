from app.storage import KeyValueStore


def test_put_and_get():
    store = KeyValueStore()

    store.put("name", "Nithin")

    assert store.get("name") == "Nithin"


def test_delete():
    store = KeyValueStore()

    store.put("name", "Nithin")
    store.delete("name")

    assert store.get("name") is None


def test_missing_key():
    store = KeyValueStore()

    assert store.get("unknown") is None