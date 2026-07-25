import pytest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
import pandas as pd

sqlalchemy = pytest.importorskip("sqlalchemy")

from ._sqlbackend import SQLBackend  # noqa: E402
from ._edge_identity import edge_identity  # noqa: E402


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


def test_context_manager_closes_connection_and_disposes_engine(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}")
    dispose = backend._engine.dispose
    backend._engine.dispose = Mock(wraps=dispose)

    with backend as entered:
        assert entered is backend
        backend.add_node("A", {})

    assert backend._closed
    assert backend._connection.closed
    backend._engine.dispose.assert_called_once_with()
    with pytest.raises(RuntimeError, match="closed"):
        backend.add_node("B", {})


def test_close_is_idempotent(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}")

    backend.close()
    backend.close()

    assert backend._closed


def test_concurrent_mutations_do_not_share_connection_simultaneously(tmp_path):
    backend = SQLBackend(
        db_url=f"sqlite:///{tmp_path / 'graph.db'}",
        sqlalchemy_kwargs={"connect_args": {"check_same_thread": False}},
    )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda node: backend.add_node(node, {}), range(20)))

    assert backend.get_node_count() == 20
    backend.close()


def test_ingest_preserves_existing_nodes_metadata_and_edges(tmp_path):
    backend = SQLBackend(
        db_url=f"sqlite:///{tmp_path / 'graph.db'}",
        directed=True,
    )
    backend.add_node("existing", {"kind": "preserved"})
    backend.add_edge("existing", "A", {"weight": 1, "label": "old"})
    edgelist = pd.DataFrame(
        {
            "source": ["existing", "A"],
            "target": ["A", "B"],
            "weight": [2, 3],
        }
    )

    backend.ingest_from_edgelist_dataframe(edgelist, "source", "target")

    assert backend.get_node_by_id("existing") == {"kind": "preserved"}
    assert backend.get_edge_by_id("existing", "A") == {
        "weight": 2,
        "label": "old",
    }
    assert backend.get_edge_by_id("A", "B") == {"weight": 3}
    assert set(backend.all_nodes_as_iterable()) == {"existing", "A", "B"}
    backend.close()


def test_ingest_preserves_primary_key_schema(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}")

    backend.ingest_from_edgelist_dataframe(
        pd.DataFrame({"source": ["A"], "target": ["B"]}),
        "source",
        "target",
    )

    assert backend._node_table.primary_key.columns.keys() == ["ID"]
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        backend._connection.execute(
            backend._node_table.insert(),
            [
                {"ID": "duplicate", "_metadata": {}},
                {"ID": "duplicate", "_metadata": {}},
            ],
        )
    backend._connection.rollback()
    backend.close()


def test_ingest_rolls_back_nodes_and_edges_on_failure(tmp_path):
    backend = SQLBackend(db_url=f"sqlite:///{tmp_path / 'graph.db'}")
    original_execute = backend._connection.execute

    def fail_edge_insert(statement, *args, **kwargs):
        if getattr(statement, "table", None) is backend._edge_table:
            raise RuntimeError("ingest failed")
        return original_execute(statement, *args, **kwargs)

    backend._connection.execute = fail_edge_insert

    with pytest.raises(RuntimeError, match="ingest failed"):
        backend.ingest_from_edgelist_dataframe(
            pd.DataFrame({"source": ["A"], "target": ["B"]}),
            "source",
            "target",
        )

    backend._connection.execute = original_execute
    assert backend.get_node_count() == 0
    assert backend.get_edge_count() == 0
    backend.close()


def test_collision_safe_edge_ids_support_ambiguous_and_long_endpoints(tmp_path):
    backend = SQLBackend(
        db_url=f"sqlite:///{tmp_path / 'graph.db'}", directed=True
    )
    long_source = "source" * 100
    long_target = "target" * 100

    first_id = backend.add_edge("a__b", "c", {"edge": 1})
    second_id = backend.add_edge("a", "b__c", {"edge": 2})
    long_id = backend.add_edge(long_source, long_target, {"edge": 3})

    assert first_id != second_id
    assert len(first_id) == len(second_id) == len(long_id) == 67
    assert backend.get_edge_by_id("a__b", "c") == {"edge": 1}
    assert backend.get_edge_by_id("a", "b__c") == {"edge": 2}
    assert backend.get_edge_by_id(long_source, long_target) == {"edge": 3}
    backend.close()


def test_existing_sql_edge_uses_primary_key_lookup(tmp_path):
    backend = SQLBackend(
        db_url=f"sqlite:///{tmp_path / 'graph.db'}", directed=True
    )
    backend.add_edge("A", "B", {"old": True})
    statements = []
    original_execute = backend._connection.execute

    def record_execute(statement, *args, **kwargs):
        statements.append(statement)
        return original_execute(statement, *args, **kwargs)

    backend._connection.execute = record_execute
    backend.add_edge("A", "B", {"new": True})

    edge_selects = [
        statement
        for statement in statements
        if getattr(statement, "is_select", False)
        and statement.get_final_froms() == [backend._edge_table]
    ]
    assert len(edge_selects) == 1
    backend.close()


def test_legacy_sql_edge_is_read_and_updated_in_place(tmp_path):
    backend = SQLBackend(
        db_url=f"sqlite:///{tmp_path / 'graph.db'}", directed=True
    )
    backend.add_nodes_from([("A", {}), ("B", {})])
    backend._connection.execute(
        backend._edge_table.insert(),
        {
            "ID": "__A__B",
            "Source": "A",
            "Target": "B",
            "_metadata": {"old": True},
        },
    )
    backend._connection.commit()

    returned_id = backend.add_edge("A", "B", {"new": True})

    assert returned_id == "__A__B"
    assert backend.get_edge_by_id("A", "B") == {"old": True, "new": True}
    assert backend.get_edge_count() == 1
    assert returned_id != edge_identity("A", "B")
    backend.close()
