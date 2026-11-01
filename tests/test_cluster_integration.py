
import os
import uuid

import httpx
import pytest


BASE_URL = os.getenv("ORBIT_BASE_URL", "http://localhost:8000")

pytestmark = pytest.mark.skipif(
    os.getenv("ORBIT_RUN_INTEGRATION") != "1",
    reason="Set ORBIT_RUN_INTEGRATION=1 to run Docker integration tests",
)


def test_three_node_replication_and_read():
    key = f"integration-{uuid.uuid4().hex}"
    value = "ORBIT DB integration test"

    with httpx.Client(timeout=15.0) as client:
        # Confirm that the leader and both followers are healthy.
        assert client.get(f"{BASE_URL}/health/ready").status_code == 200
        assert client.get("http://localhost:8001/health/ready").status_code == 200
        assert client.get("http://localhost:8002/health/ready").status_code == 200

        # Write through the leader using a JSON request body.
        write = client.put(
            f"{BASE_URL}/kv/{key}",
            json={"value": value},
        )

        assert write.status_code == 200

        write_data = write.json()

        assert write_data["value"] == value
        assert write_data["version"] == 1
        assert write_data["quorum_achieved"] is True
        assert write_data["successful_nodes"] == 3

        operation_id = write_data["operation_id"]

        # Verify that follower-1 received the same record.
        follower_1 = client.get(
            f"http://localhost:8001/internal/record/{key}"
        )

        assert follower_1.status_code == 200

        record_1 = follower_1.json()["record"]

        assert record_1["value"] == value
        assert record_1["version"] == write_data["version"]
        assert record_1["operation_id"] == operation_id

        # Verify that follower-2 received the same record.
        follower_2 = client.get(
            f"http://localhost:8002/internal/record/{key}"
        )

        assert follower_2.status_code == 200

        record_2 = follower_2.json()["record"]

        assert record_2["value"] == value
        assert record_2["version"] == write_data["version"]
        assert record_2["operation_id"] == operation_id

        # Read through the leader.
        read = client.get(f"{BASE_URL}/kv/{key}")

        assert read.status_code == 200

        read_data = read.json()

        assert read_data["key"] == key
        assert read_data["value"] == value
        assert read_data["version"] == write_data["version"]
        assert read_data["quorum_achieved"] is True

        # Delete the key through the leader.
        delete = client.delete(f"{BASE_URL}/kv/{key}")

        assert delete.status_code == 200

        delete_data = delete.json()

        assert delete_data["status"] == "deleted"
        assert delete_data["quorum_achieved"] is True
        assert delete_data["version"] == write_data["version"] + 1

        # Confirm that the deleted key is no longer readable.
        deleted_read = client.get(f"{BASE_URL}/kv/{key}")

        assert deleted_read.status_code == 404

        # Confirm that the tombstone reached both followers.
        for follower_url in (
            "http://localhost:8001",
            "http://localhost:8002",
        ):
            response = client.get(
                f"{follower_url}/internal/record/{key}"
            )

            assert response.status_code == 200

            tombstone = response.json()["record"]

            assert tombstone["deleted"] == 1
            assert tombstone["value"] is None
            assert tombstone["version"] == delete_data["version"]
