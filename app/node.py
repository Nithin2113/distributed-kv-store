from app.storage import KeyValueStore


class Node:
    def __init__(self, node_id, role, port, storage_path):
        self.node_id = node_id
        self.role = role
        self.port = port
        self.storage = KeyValueStore(storage_path)
        self.is_alive = True

    def get_info(self):
        return {
            "node_id": self.node_id,
            "role": self.role,
            "port": self.port,
            "is_alive": self.is_alive,
        }