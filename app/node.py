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

    def replicate_put(self, key, value):
        results = []

        for follower in self.followers:
            try:
                response = httpx.put(
                    f"{follower}/kv/{key}",
                    params={"value": value},
                    timeout=2.0,
                )

                results.append({
                    "node": follower,
                    "success": response.status_code == 200,
                })

            except httpx.RequestError:
                results.append({
                    "node": follower,
                    "success": False,
                })

        successful_replicas = sum(
            result["success"] for result in results
        )

        successful_nodes = 1 + successful_replicas

        return {
            "results": results,
            "successful_nodes": successful_nodes,
        }

    def read_from_followers(self, key):
        results = []

        for follower in self.followers:
            try:
                response = httpx.get(
                    f"{follower}/kv/{key}",
                    timeout=2.0,
                )

                if response.status_code == 200:
                    data = response.json()

                    results.append({
                        "node": follower,
                        "success": True,
                        "value": data["value"],
                    })
                else:
                    results.append({
                        "node": follower,
                        "success": False,
                    })

            except httpx.RequestError:
                results.append({
                    "node": follower,
                    "success": False,
                })

        successful_replicas = sum(
            1 for result in results if result["success"]
        )

        successful_nodes = 1 + successful_replicas

        return {
            "results": results,
            "successful_nodes": successful_nodes,
        }