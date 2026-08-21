from app.node import Node


def test_node_information():
    node = Node(
        node_id="node-1",
        role="leader",
        port=8000,
        storage_path="test-data/node1/store.json",
    )

    info = node.get_info()

    assert info["node_id"] == "node-1"
    assert info["role"] == "leader"
    assert info["port"] == 8000
    assert info["is_alive"] is True