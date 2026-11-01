
import httpx
from datetime import datetime, timezone

from app.storage import KeyValueStore


class Node:
    def __init__(
        self,
        node_id,
        role,
        port,
        storage_path,
        followers=None,
        heartbeat_timeout=1.0,
        failure_threshold=2,
    ):
        self.node_id = node_id
        self.role = role
        self.port = port
        self.storage = KeyValueStore(storage_path)
        self.is_alive = True
        self.followers = followers or []

        self.heartbeat_timeout = heartbeat_timeout
        self.failure_threshold = failure_threshold

        self.follower_health = {
            follower: {
                "state": "UNKNOWN",
                "consecutive_failures": 0,
                "last_seen": None,
                "last_error": None,
            }
            for follower in self.followers
        }

    def _timestamp(self):
        return datetime.now(timezone.utc).isoformat()

    def get_info(self):
        return {
            "node_id": self.node_id,
            "role": self.role,
            "port": self.port,
            "is_alive": self.is_alive,
        }

    def get_health(self):
        return {
            "node": self.get_info(),
            "followers": self.follower_health,
            "total_followers": len(self.followers),
            "healthy_followers": sum(
                1
                for item in self.follower_health.values()
                if item["state"] == "HEALTHY"
            ),
        }

    def synchronize_follower(self, follower):
        """
        Send the leader's complete current keyspace to a recovered follower.
        Tombstones are included so deleted keys cannot reappear.
        """
        records = self.storage.list_records()

        for record in records:
            try:
                response = httpx.post(
                    f"{follower}/internal/replicate",
                    json=record,
                    timeout=2.0,
                )

                try:
                    data = response.json()
                except ValueError:
                    data = {}

                if (
                    response.status_code != 200
                    or data.get("success") is not True
                ):
                    reason = data.get(
                        "reason",
                        f"http_{response.status_code}",
                    )
                    return {
                        "success": False,
                        "records_sent": 0,
                        "reason": reason,
                    }

            except httpx.RequestError as exc:
                return {
                    "success": False,
                    "records_sent": 0,
                    "reason": str(exc),
                }

        return {
            "success": True,
            "records_sent": len(records),
            "reason": "synchronization_complete",
        }

    def check_followers(self):
        """Probe followers and synchronize replicas after recovery."""
        if self.role != "leader":
            return self.get_health()

        for follower in self.followers:
            health = self.follower_health[follower]

            try:
                response = httpx.get(
                    f"{follower}/health/live",
                    timeout=self.heartbeat_timeout,
                )
                response.raise_for_status()

                data = response.json()

                if data.get("status") != "alive":
                    raise RuntimeError("Follower did not report alive")

                previous_state = health["state"]

                health["consecutive_failures"] = 0
                health["last_seen"] = self._timestamp()
                health["last_error"] = None

                if previous_state in ("DOWN", "SUSPECT"):
                    # First successful heartbeat: begin recovery.
                    health["state"] = "RECOVERING"

                elif previous_state == "RECOVERING":
                    # Synchronize before declaring the replica healthy.
                    result = self.synchronize_follower(follower)

                    if result["success"]:
                        health["state"] = "HEALTHY"
                        health["last_error"] = None
                        health["last_sync"] = self._timestamp()
                        health["records_synchronized"] = result["records_sent"]
                    else:
                        health["state"] = "RECOVERING"
                        health["last_error"] = (
                            f"Synchronization failed: {result['reason']}"
                        )

                else:
                    health["state"] = "HEALTHY"

            except (
                httpx.RequestError,
                httpx.HTTPStatusError,
                ValueError,
                RuntimeError,
            ) as exc:
                health["consecutive_failures"] += 1
                health["last_error"] = str(exc)

                if health["consecutive_failures"] >= self.failure_threshold:
                    health["state"] = "DOWN"
                else:
                    health["state"] = "SUSPECT"

        return self.get_health()

    def replicate_record(self, record):
        """Send the leader's exact versioned record to every follower."""
        results = []

        for follower in self.followers:
            try:
                response = httpx.post(
                    f"{follower}/internal/replicate",
                    json=record,
                    timeout=2.0,
                )

                try:
                    data = response.json()
                except ValueError:
                    data = {}

                success = (
                    response.status_code == 200
                    and data.get("success") is True
                )

                results.append({
                    "node": follower,
                    "success": success,
                    "reason": data.get(
                        "reason",
                        "replication_failed",
                    ),
                })

            except httpx.RequestError as exc:
                results.append({
                    "node": follower,
                    "success": False,
                    "reason": str(exc),
                })

        successful_replicas = sum(
            1 for result in results if result["success"]
        )

        return {
            "results": results,
            "successful_nodes": 1 + successful_replicas,
        }

    def replicate_put(self, key, value):
        record = self.storage.put(key, value)
        return self.replicate_record(record)

    def replicate_delete(self, key):
        old_value = self.storage.delete(key)

        if old_value is None:
            return None

        record = self.storage.get_record(key)
        replication = self.replicate_record(record)

        return {
            "old_value": old_value,
            "record": record,
            "replication": replication,
        }

    def read_from_followers(self, key):
        results = []

        for follower in self.followers:
            try:
                response = httpx.get(
                    f"{follower}/internal/record/{key}",
                    timeout=2.0,
                )

                if response.status_code == 200:
                    data = response.json()

                    results.append({
                        "node": follower,
                        "success": True,
                        "record": data.get("record"),
                    })
                else:
                    results.append({
                        "node": follower,
                        "success": False,
                        "reason": f"http_{response.status_code}",
                    })

            except httpx.RequestError as exc:
                results.append({
                    "node": follower,
                    "success": False,
                    "reason": str(exc),
                })

        successful_replicas = sum(
            1 for result in results if result["success"]
        )

        return {
            "results": results,
            "successful_nodes": 1 + successful_replicas,
        }

    def repair_follower(self, follower, record):
        try:
            response = httpx.post(
                f"{follower}/internal/replicate",
                json=record,
                timeout=2.0,
            )

            try:
                data = response.json()
            except ValueError:
                data = {}

            return {
                "node": follower,
                "success": (
                    response.status_code == 200
                    and data.get("success") is True
                ),
                "reason": data.get("reason", "repair_failed"),
            }

        except httpx.RequestError as exc:
            return {
                "node": follower,
                "success": False,
                "reason": str(exc),
            }
