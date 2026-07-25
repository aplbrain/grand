from contextlib import nullcontext

import pandas as pd
import pytest

from ._dataframe import DataFrameBackend
from ._networkx import NetworkXBackend

try:
    from ._sqlbackend import SQLBackend
except ImportError:
    SQLBackend = None


pytestmark = pytest.mark.benchmark


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param(NetworkXBackend, id="networkx"),
        pytest.param(DataFrameBackend, id="dataframe"),
        pytest.param(
            SQLBackend,
            marks=pytest.mark.skipif(SQLBackend is None, reason="SQL unavailable"),
            id="sql",
        ),
    ],
)
def test_high_degree_neighbor_traversal(backend):
    kwargs = {"db_url": "sqlite:///:memory:"} if backend is SQLBackend else {}
    graph = backend(directed=True, **kwargs)
    transaction = getattr(graph, "transaction", None)
    with transaction() if transaction else nullcontext():
        for target in range(500):
            graph.add_edge("hub", target, {})

    assert len(list(graph.get_node_neighbors("hub"))) == 500
    if backend is SQLBackend:
        graph.close()


@pytest.mark.skipif(SQLBackend is None, reason="SQL unavailable")
def test_sql_grouped_transaction_node_insertion():
    backend = SQLBackend(db_url="sqlite:///:memory:")

    with backend.transaction():
        for node in range(500):
            backend.add_node(node, {})

    assert backend.get_node_count() == 500
    backend.close()


@pytest.mark.skipif(SQLBackend is None, reason="SQL unavailable")
def test_sql_bulk_edge_insertion():
    backend = SQLBackend(db_url="sqlite:///:memory:", directed=True)

    backend.add_edges_from((node, node + 1) for node in range(500))

    assert backend.get_node_count() == 501
    assert backend.get_edge_count() == 500
    backend.close()


def test_dataframe_incremental_edge_insertion():
    backend = DataFrameBackend(directed=True)

    for node in range(250):
        backend.add_edge(node, node + 1, {})

    assert backend.get_edge_count() == 250


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param(NetworkXBackend, id="networkx"),
        pytest.param(DataFrameBackend, id="dataframe"),
        pytest.param(
            SQLBackend,
            marks=pytest.mark.skipif(SQLBackend is None, reason="SQL unavailable"),
            id="sql",
        ),
    ],
)
def test_dataframe_ingestion_with_metadata(backend):
    kwargs = {"db_url": "sqlite:///:memory:"} if backend is SQLBackend else {}
    graph = backend(directed=True, **kwargs)
    edgelist = pd.DataFrame(
        {
            "source": range(500),
            "target": range(1, 501),
            "weight": range(500),
            "kind": ["edge"] * 500,
        }
    )

    graph.ingest_from_edgelist_dataframe(edgelist, "source", "target")

    assert graph.get_edge_count() == 500
    if backend is SQLBackend:
        graph.close()
