import importlib

import pytest

pytest.importorskip("gremlin_python")
GremlinBackend = importlib.import_module("grand.backends._gremlin").GremlinBackend


class RecordingTraversal:
    def __init__(self, result, calls=None):
        self.result = result
        self.calls = calls if calls is not None else []

    def __getattr__(self, name):
        def step(*args):
            self.calls.append((name, args))
            return self

        return step

    def toList(self):
        self.calls.append(("toList", ()))
        return self.result


def test_predecessors_start_from_requested_node():
    traversal = RecordingTraversal(["parent"])
    backend = GremlinBackend(traversal)

    assert backend.get_node_predecessors("child") == ["parent"]
    assert [(name, args) for name, args in traversal.calls] == [
        ("V", ()),
        ("has", ("__id", "child")),
        ("in_", ()),
        ("values", ("__id",)),
        ("toList", ()),
    ]


def test_predecessors_with_metadata_return_edge_metadata():
    traversal = RecordingTraversal(
        [{"source": "parent", "target": "child", "properties": {"weight": 2}}]
    )
    backend = GremlinBackend(traversal)

    assert backend.get_node_predecessors("child", include_metadata=True) == {
        "parent": {"weight": 2}
    }


@pytest.mark.parametrize("metadata", [{}, {"weight": 2}])
def test_get_edge_returns_metadata_dictionary(metadata):
    traversal = RecordingTraversal([metadata])
    backend = GremlinBackend(traversal)

    assert backend.get_edge_by_id("source", "target") == metadata
    assert ("valueMap", ()) in traversal.calls


def test_missing_edge_raises_key_error():
    backend = GremlinBackend(RecordingTraversal([]))

    with pytest.raises(KeyError):
        backend.get_edge_by_id("source", "target")


def test_add_edge_updates_existing_edge():
    traversal = RecordingTraversal([{}])
    backend = GremlinBackend(traversal)

    backend.add_edge("source", "target", {"weight": 2})

    assert ("addE", ("__edge",)) not in traversal.calls
    assert ("property", ("weight", 2)) in traversal.calls


def test_add_edge_creates_missing_edge(monkeypatch):
    traversal = RecordingTraversal([])
    backend = GremlinBackend(traversal)
    monkeypatch.setattr(backend, "get_edge_by_id", lambda *args: (_ for _ in ()).throw(KeyError()))
    monkeypatch.setattr(backend, "has_node", lambda node: True)

    backend.add_edge("source", "target", {})

    assert ("addE", ("__edge",)) in traversal.calls
