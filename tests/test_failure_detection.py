
from unittest.mock import Mock

import httpx

from app.node import Node


def make_node():
    return Node(
        node_id="node-1",
        role="leader",
        port=8000,
        storage_path=":memory:",
        followers=["http://follower-1:8000"],
        heartbeat_timeout=0.1,
        failure_threshold=2,
    )


def healthy_response():
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "status": "alive",
        "node_id": "node-2",
    }
    return response


def successful_replication_response():
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "success": True,
        "reason": "applied",
    }
    return response


def test_healthy_follower_is_detected(monkeypatch):
    node = make_node()

    monkeypatch.setattr(
        "app.node.httpx.get",
        lambda *args, **kwargs: healthy_response(),
    )

    result = node.check_followers()
    follower = result["followers"]["http://follower-1:8000"]

    assert follower["state"] == "HEALTHY"
    assert follower["consecutive_failures"] == 0
    node.storage.close()


def test_first_failure_marks_follower_suspect(monkeypatch):
    node = make_node()

    def failed_request(*args, **kwargs):
        raise httpx.ConnectError("follower unavailable")

    monkeypatch.setattr(
        "app.node.httpx.get",
        failed_request,
    )

    result = node.check_followers()
    follower = result["followers"]["http://follower-1:8000"]

    assert follower["state"] == "SUSPECT"
    assert follower["consecutive_failures"] == 1
    node.storage.close()


def test_repeated_failures_mark_follower_down(monkeypatch):
    node = make_node()

    def failed_request(*args, **kwargs):
        raise httpx.ConnectError("follower unavailable")

    monkeypatch.setattr(
        "app.node.httpx.get",
        failed_request,
    )

    node.check_followers()
    result = node.check_followers()
    follower = result["followers"]["http://follower-1:8000"]

    assert follower["state"] == "DOWN"
    assert follower["consecutive_failures"] == 2
    node.storage.close()


def test_recovered_follower_transitions_to_recovering_then_healthy(monkeypatch):
    node = make_node()

    def failed_request(*args, **kwargs):
        raise httpx.ConnectError("follower unavailable")

    monkeypatch.setattr(
        "app.node.httpx.get",
        failed_request,
    )

    node.check_followers()
    node.check_followers()

    monkeypatch.setattr(
        "app.node.httpx.get",
        lambda *args, **kwargs: healthy_response(),
    )

    first_result = node.check_followers()
    follower = first_result["followers"]["http://follower-1:8000"]
    assert follower["state"] == "RECOVERING"

    second_result = node.check_followers()
    follower = second_result["followers"]["http://follower-1:8000"]
    assert follower["state"] == "HEALTHY"

    node.storage.close()


def test_recovery_synchronizes_all_records_including_tombstones(monkeypatch):
    node = make_node()

    node.storage.put("existing-key", "existing-value")
    node.storage.put("updated-key", "old-value")
    node.storage.put("updated-key", "new-value")
    node.storage.put("deleted-key", "temporary-value")
    node.storage.delete("deleted-key")

    node.follower_health["http://follower-1:8000"]["state"] = "DOWN"

    monkeypatch.setattr(
        "app.node.httpx.get",
        lambda *args, **kwargs: healthy_response(),
    )

    sent_records = []

    def mock_post(url, json, timeout):
        sent_records.append(json)
        return successful_replication_response()

    monkeypatch.setattr(
        "app.node.httpx.post",
        mock_post,
    )

    first_result = node.check_followers()
    follower = first_result["followers"]["http://follower-1:8000"]

    assert follower["state"] == "RECOVERING"
    assert len(sent_records) == 0

    second_result = node.check_followers()
    follower = second_result["followers"]["http://follower-1:8000"]

    assert follower["state"] == "HEALTHY"
    assert follower["records_synchronized"] == 3

    records_by_key = {
        record["key"]: record for record in sent_records
    }

    assert records_by_key["existing-key"]["value"] == "existing-value"
    assert records_by_key["updated-key"]["value"] == "new-value"
    assert records_by_key["updated-key"]["version"] == 2
    assert records_by_key["deleted-key"]["deleted"] == 1
    assert records_by_key["deleted-key"]["value"] is None

    node.storage.close()


def test_failed_synchronization_keeps_follower_recovering(monkeypatch):
    node = make_node()

    node.storage.put("recovery-key", "recovery-value")
    node.follower_health["http://follower-1:8000"]["state"] = "DOWN"

    monkeypatch.setattr(
        "app.node.httpx.get",
        lambda *args, **kwargs: healthy_response(),
    )

    first_result = node.check_followers()
    assert (
        first_result["followers"]["http://follower-1:8000"]["state"]
        == "RECOVERING"
    )

    failed_response = Mock()
    failed_response.status_code = 409
    failed_response.json.return_value = {
        "success": False,
        "reason": "version_conflict",
    }

    monkeypatch.setattr(
        "app.node.httpx.post",
        lambda *args, **kwargs: failed_response,
    )

    second_result = node.check_followers()
    follower = second_result["followers"]["http://follower-1:8000"]

    assert follower["state"] == "RECOVERING"
    assert "version_conflict" in follower["last_error"]

    node.storage.close()
