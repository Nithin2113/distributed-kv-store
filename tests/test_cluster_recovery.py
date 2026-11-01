
import os
import subprocess
import time
import uuid

import httpx
import pytest


BASE_URL = os.getenv("ORBIT_BASE_URL", "http://localhost:8000")
FOLLOWER_URL = "http://localhost:8002"

pytestmark = pytest.mark.skipif(
    os.getenv("ORBIT_RUN_INTEGRATION") != "1",
    reason="Set ORBIT_RUN_INTEGRATION=1 to run Docker integration tests",
)


def test_offline_follower_recovers_through_read_repair():
    key = f"recovery-{uuid.uuid4().hex}"
    value = "ORBIT DB recovered this replica"

    with httpx.Client(timeout=15.0) as client:
        # Confirm the cluster is initially available.
        assert client.get(f"{BASE_URL}/health/ready").status_code == 200
        assert client.get(f"http://localhost:8001/health/ready").status_code == 200
        assert client.get(f"{FOLLOWER_URL}/health/ready").status_code == 200

        write_data = None

        try:
            # Take follower-2 offline.
            subprocess.run(
                ["docker", "compose", "stop", "follower-2"],
                check=True,
                capture_output=True,
                text=True,
            )

            # Write while one follower is unavailable.
            write = client.put(
                f"{BASE_URL}/kv/{key}",
                json={"value": value},
            )

            assert write.status_code == 200

            write_data = write.json()

            assert write_data["quorum_achieved"] is True
            assert write_data["successful_nodes"] == 2

            # Confirm the available follower received the write.
            follower_1 = client.get(
                f"http://localhost:8001/internal/record/{key}"
            )

            assert follower_1.status_code == 200

            follower_record = follower_1.json()["record"]

            assert follower_record["value"] == value
            assert follower_record["version"] == write_data["version"]
            assert follower_record["operation_id"] == write_data["operation_id"]

        finally:
            # Always bring follower-2 back, even if an assertion fails.
            subprocess.run(
                ["docker", "compose", "start", "follower-2"],
                check=True,
                capture_output=True,
                text=True,
            )

        # Wait for the restarted follower to become healthy.
        deadline = time.time() + 30

        while time.time() < deadline:
            try:
                response = client.get(
                    f"{FOLLOWER_URL}/health/ready",
                    timeout=2.0,
                )

                if response.status_code == 200:
                    break

            except httpx.RequestError:
                pass

            time.sleep(1)

        else:
            pytest.fail(
                "Follower-2 did not become healthy within 30 seconds"
            )

        # Read from the leader. This should trigger read repair.
        read = client.get(f"{BASE_URL}/kv/{key}")

        assert read.status_code == 200

        read_data = read.json()

        assert read_data["value"] == value
        assert read_data["version"] == write_data["version"]

        # Confirm that follower-2 was repaired.
        repairs = read_data["repairs"]

        repaired = any(
            item["node"] == "http://follower-2:8000"
            and item["success"] is True
            for item in repairs
        )

        assert repaired, f"Follower-2 was not repaired: {repairs}"

        # Verify the recovered follower now stores the latest record.
        recovered = client.get(
            f"{FOLLOWER_URL}/internal/record/{key}"
        )

        assert recovered.status_code == 200

        recovered_record = recovered.json()["record"]

        assert recovered_record["value"] == value
        assert recovered_record["version"] == write_data["version"]
        assert recovered_record["operation_id"] == write_data["operation_id"]
