import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")

from ._sqlbackend import SQLBackend  # noqa: E402


def test_mutations_persist_without_explicit_commit(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'graph.db'}"
    backend = SQLBackend(db_url=db_url, directed=True)

    backend.add_edge("A", "B", {"weight": 1})
    backend.close()

    reopened = SQLBackend(db_url=db_url, directed=True)
    assert set(reopened.all_nodes_as_iterable()) == {"A", "B"}
    assert reopened.get_edge_by_id("A", "B") == {"weight": 1}
    reopened.close()


def test_remove_node_persists_without_explicit_commit(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'graph.db'}"
    backend = SQLBackend(db_url=db_url, directed=True)
    backend.add_edge("A", "B", {})

    backend.remove_node("A")
    backend.close()

    reopened = SQLBackend(db_url=db_url, directed=True)
    assert not reopened.has_node("A")
    assert not reopened.has_edge("A", "B")
    reopened.close()


def test_existing_edge_update_persists_without_explicit_commit(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'graph.db'}"
    backend = SQLBackend(db_url=db_url, directed=True)
    backend.add_edge("A", "B", {"weight": 1, "kind": "old"})

    backend.add_edge("A", "B", {"weight": 2})
    backend.close()

    reopened = SQLBackend(db_url=db_url, directed=True)
    assert reopened.get_edge_by_id("A", "B") == {"weight": 2, "kind": "old"}
    reopened.close()


def test_transaction_context_commits_grouped_mutations(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'graph.db'}"
    backend = SQLBackend(db_url=db_url, directed=True)

    with backend.transaction():
        backend.add_node("A", {})
        backend.add_node("B", {})
        backend.add_edge("A", "B", {})
    backend.close()

    reopened = SQLBackend(db_url=db_url, directed=True)
    assert reopened.has_edge("A", "B")
    reopened.close()


def test_transaction_context_rolls_back_grouped_mutations(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}")

    with pytest.raises(RuntimeError, match="abort"):
        with backend.transaction():
            backend.add_node("A", {})
            raise RuntimeError("abort")

    assert not backend.has_node("A")
    backend.close()


def test_add_edges_from_creates_endpoints_and_accepts_two_tuples(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}", directed=True)

    backend.add_edges_from([("A", "B"), ("B", "C", {"weight": 2})])

    assert set(backend.all_nodes_as_iterable()) == {"A", "B", "C"}
    assert backend.has_edge("A", "B")
    assert backend.get_edge_by_id("B", "C") == {"weight": 2}
    backend.close()


def test_add_edges_from_merges_existing_edge_metadata(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}", directed=True)
    backend.add_edge("A", "B", {"weight": 1, "kind": "existing"})

    backend.add_edges_from([("A", "B", {"weight": 2})], batch=True)

    assert backend.get_edge_by_id("A", "B") == {
        "weight": 2,
        "kind": "existing",
        "batch": True,
    }
    backend.close()


def test_add_edges_from_rolls_back_entire_batch_on_failure(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}", directed=True)
    original_execute = backend._connection.execute
    def fail_edge_insert(statement, *args, **kwargs):
        if getattr(statement, "table", None) is backend._edge_table:
            raise RuntimeError("edge batch failed")
        return original_execute(statement, *args, **kwargs)

    backend._connection.execute = fail_edge_insert

    with pytest.raises(RuntimeError, match="edge batch failed"):
        backend.add_edges_from([("A", "B"), ("B", "C")])

    backend._connection.execute = original_execute
    assert backend.get_node_count() == 0
    assert backend.get_edge_count() == 0
    backend.close()


def test_add_edge_rolls_back_created_nodes_when_edge_insert_fails(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}", directed=True)
    original_execute = backend._connection.execute

    def fail_edge_insert(statement, *args, **kwargs):
        if getattr(statement, "table", None) is backend._edge_table:
            raise RuntimeError("edge insert failed")
        return original_execute(statement, *args, **kwargs)

    backend._connection.execute = fail_edge_insert

    with pytest.raises(RuntimeError, match="edge insert failed"):
        backend.add_edge("A", "B", {})

    backend._connection.execute = original_execute
    assert not backend.has_node("A")
    assert not backend.has_node("B")
    backend.close()
