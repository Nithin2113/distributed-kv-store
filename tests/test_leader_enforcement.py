
import pytest
from fastapi import HTTPException

import app.main as main


class FakeStorage:
    def put(self, key, value):
        raise AssertionError("Follower must not write locally")

    def delete(self, key):
        raise AssertionError("Follower must not delete locally")

    def apply_record(self, record):
        return {
            "success": True,
            "reason": "applied",
        }


def test_follower_rejects_client_put(monkeypatch):
    monkeypatch.setattr(main.node, "role", "follower")
    monkeypatch.setattr(main.node, "storage", FakeStorage())

    with pytest.raises(HTTPException) as error:
        main.put_value(
            "test-key",
            main.PutRequest(value="test"),
        )

    assert error.value.status_code == 403


def test_follower_rejects_client_delete(monkeypatch):
    monkeypatch.setattr(main.node, "role", "follower")
    monkeypatch.setattr(main.node, "storage", FakeStorage())

    with pytest.raises(HTTPException) as error:
        main.delete_value("test-key")

    assert error.value.status_code == 403


def test_leader_rejects_internal_replication(monkeypatch):
    monkeypatch.setattr(main.node, "role", "leader")

    with pytest.raises(HTTPException) as error:
        main.receive_replica({})

    assert error.value.status_code == 403


def test_follower_accepts_internal_replication(monkeypatch):
    monkeypatch.setattr(main.node, "role", "follower")
    monkeypatch.setattr(main.node, "storage", FakeStorage())

    result = main.receive_replica({"key": "test"})

    assert result["success"] is True
    assert result["reason"] == "applied"
