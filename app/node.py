
import httpx

from app.storage import KeyValueStore


class Node:
    def __init__(self, node_id, role, port, storage_path, followers=None):
        self.node_id = node_id
        self.role = role
        self.port = port
        self.storage = KeyValueStore(storage_path)
        self.is_alive = True
        self.followers = followers or []

    def get_info(self):
        return {
            "node_id": self.node_id,
            "role": self.role,
            "port": self.port,
            "is_alive": self.is_alive,
        }

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
                    "reason": data.get("reason", "replication_failed"),
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
        """Compatibility helper: persist locally, then replicate."""
        record = self.storage.put(key, value)
        return self.replicate_record(record)

    def replicate_delete(self, key):
        """Create a local tombstone and replicate it."""
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
        """Fetch full versioned records from followers."""
        results = []

        for follower in self.followers:
            try:
                response = httpx.get(
                    f"{follower}/internal/record/{key}",
                    timeout=2.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    record = data.get("record")

                    results.append({
                        "node": follower,
                        "success": True,
                        "record": record,
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
        """Send the newest known record to a stale follower."""
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
