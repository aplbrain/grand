import pytest
import os

import networkx as nx

from . import NetworkXBackend, DataFrameBackend

try:
    from ._dynamodb import DynamoDBBackend

    _CAN_IMPORT_DYNAMODB = True
except ImportError:
    _CAN_IMPORT_DYNAMODB = False

try:
    from ._igraph import IGraphBackend

    _CAN_IMPORT_IGRAPH = True
except ImportError:
    _CAN_IMPORT_IGRAPH = False

try:
    from ._networkit import NetworkitBackend

    _CAN_IMPORT_NETWORKIT = True
except ImportError:
    _CAN_IMPORT_NETWORKIT = False

try:
    from ._sqlbackend import SQLBackend

    _CAN_IMPORT_SQL = True
except ImportError:
    _CAN_IMPORT_SQL = False

from .. import Graph


backend_test_params = [
    pytest.param(
        (NetworkXBackend, {}),
        marks=pytest.mark.skipif(
            os.environ.get("TEST_NETWORKXBACKEND", default="1") != "1",
            reason="NetworkX Backend skipped because $TEST_NETWORKXBACKEND != 1.",
        ),
        id="NetworkXBackend",
    ),
]
backend_test_params = [
    pytest.param(
        (DataFrameBackend, {}),
        marks=pytest.mark.skipif(
            os.environ.get("TEST_DATAFRAMEBACKEND", default="1") != "1",
            reason="DataFrameBackend skipped because $TEST_DATAFRAMEBACKEND != 1.",
        ),
        id="DataFrameBackend",
    ),
]

if _CAN_IMPORT_DYNAMODB:
    backend_test_params.append(
        pytest.param(
            (DynamoDBBackend, {}),
            marks=pytest.mark.skipif(
                os.environ.get("TEST_DYNAMODB", default="1") != "1",
                reason="DynamoDB Backend skipped because $TEST_DYNAMODB != 0 or boto3 is not installed",
            ),
            id="DynamoDBBackend",
        ),
    )

if _CAN_IMPORT_SQL:
    backend_test_params.append(
        pytest.param(
            (SQLBackend, {"db_url": "sqlite:///:memory:"}),
            marks=pytest.mark.skipif(
                os.environ.get("TEST_SQLBACKEND", default="1") != "1",
                reason="SQL Backend skipped because $TEST_SQLBACKEND != 1 or sqlalchemy is not installed.",
            ),
            id="SQLBackend",
        ),
    )
if _CAN_IMPORT_IGRAPH:
    backend_test_params.append(
        pytest.param(
            (IGraphBackend, {}),
            marks=pytest.mark.skipif(
                os.environ.get("TEST_IGRAPHBACKEND", default="1") != "1",
                reason="IGraph Backend skipped because $TEST_IGRAPHBACKEND != 1 or igraph is not installed.",
            ),
            id="IGraphBackend",
        ),
    )
if _CAN_IMPORT_NETWORKIT:
    backend_test_params.append(
        pytest.param(
            (NetworkitBackend, {}),
            marks=pytest.mark.skipif(
                os.environ.get("TEST_NETWORKIT", default="1") != "1",
                reason="Networkit Backend skipped because $TEST_NETWORKIT != 1 or networkit is not installed.",
            ),
            id="NetworkitBackend",
        ),
    )

if os.environ.get("TEST_NETWORKITBACKEND") == "1":
    from ._networkit import NetworkitBackend

    backend_test_params.append(
        pytest.param(
            (NetworkitBackend, {}),
            marks=pytest.mark.skipif(
                os.environ.get("TEST_NETWORKITBACKEND") != "1",
                reason="Networkit Backend skipped because $TEST_NETWORKITBACKEND != 1.",
            ),
            id="NetworkitBackend",
        ),
    )

if os.environ.get("TEST_IGRAPHBACKEND") == "1":
    from ._igraph import IGraphBackend

    backend_test_params.append(
        pytest.param(
            (IGraphBackend, {}),
            marks=pytest.mark.skipif(
                os.environ.get("TEST_IGRAPHBACKEND") != "1",
                reason="Networkit Backend skipped because $TEST_IGRAPHBACKEND != 1.",
            ),
            id="IGraphBackend",
        ),
    )


# @pytest.mark.parametrize("backend", backend_test_params)
class TestBackendPersistence:
    def test_sqlite_persistence(self):
        if not _CAN_IMPORT_SQL:
            return

        dbpath = "grand_peristence_test_temp.db"
        url = "sqlite:///" + dbpath

        # arrange
        backend = SQLBackend(db_url=url, directed=True)
        node0 = backend.add_node("A", {"foo": "bar"})
        backend.commit()
        backend.close()
        # act
        backend = SQLBackend(db_url=url, directed=True)
        nodes = list(backend.all_nodes_as_iterable())
        # assert
        assert node0 in nodes
        # cleanup
        os.remove(dbpath)


@pytest.mark.parametrize("backend", backend_test_params)
class TestBackend:
    def test_can_create(self, backend):
        backend, kwargs = backend
        backend(**kwargs)

    def test_can_create_directed_and_undirected_backends(self, backend):
        backend, kwargs = backend
        b = backend(directed=True, **kwargs)
        assert b.is_directed() == True

        b = backend(directed=False, **kwargs)
        assert b.is_directed() == False

    def test_can_add_node(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        nxG = nx.Graph()
        G.nx.add_node("A", k="v")
        nxG.add_node("A", k="v")
        assert len(G.nx.nodes()) == len(nxG.nodes())
        G.nx.add_node("B", k="v")
        nxG.add_node("B", k="v")
        assert len(G.nx.nodes()) == len(nxG.nodes())

    def test_can_update_node(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        G.nx.add_node("A", k="v", z=3)
        G.nx.add_node("A", k="v2", x=4)
        assert G.nx.nodes["A"]["k"] == "v2"
        assert G.nx.nodes["A"]["x"] == 4
        assert G.nx.nodes["A"]["z"] == 3

    def test_can_add_edge(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        nxG = nx.Graph()
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert len(G.nx.edges()) == len(nxG.edges())
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert len(G.nx.edges()) == len(nxG.edges())

    def test_can_update_edge(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        G.nx.add_edge("A", "B", k="v", z=3)
        G.nx.add_edge("A", "B", k="v2", x=4)
        assert G.nx.get_edge_data("A", "B")["k"] == "v2"
        assert G.nx.get_edge_data("A", "B")["x"] == 4
        assert G.nx.get_edge_data("A", "B")["z"] == 3
        assert len(G.nx.nodes()) == 2

    def test_can_get_node(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        nxG = nx.Graph()
        md = dict(k="B")
        G.nx.add_node("A", **md)
        nxG.add_node("A", **md)
        assert G.nx.nodes["A"] == nxG.nodes["A"]

    def test_can_get_edge(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        nxG = nx.Graph()
        md = {"k": "B"}
        G.nx.add_edge("A", "B", **md)
        nxG.add_edge("A", "B", **md)
        assert G.nx.get_edge_data("A", "B") == nxG.get_edge_data("A", "B")

    def test_can_get_neighbors(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        nxG = nx.Graph()
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert sorted([i for i in G.nx.neighbors("A")]) == sorted(
            [i for i in nxG.neighbors("A")]
        )
        assert sorted([i for i in G.nx.neighbors("B")]) == sorted(
            [i for i in nxG.neighbors("B")]
        )
        G.nx.add_edge("A", "C")
        nxG.add_edge("A", "C")
        assert sorted([i for i in G.nx.neighbors("A")]) == sorted(
            [i for i in nxG.neighbors("A")]
        )
        assert sorted([i for i in G.nx.neighbors("B")]) == sorted(
            [i for i in nxG.neighbors("B")]
        )
        assert sorted([i for i in G.nx.neighbors("C")]) == sorted(
            [i for i in nxG.neighbors("C")]
        )

    def test_undirected_adj(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        nxG = nx.Graph()
        assert G.nx._adj == nxG._adj
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert G.nx._adj == nxG._adj

    def test_directed_adj(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=True, **kwargs))
        nxG = nx.DiGraph()
        assert G.nx._adj == nxG._adj
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert G.nx._adj == nxG._adj

    def test_can_traverse_undirected_graph(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        nxG = nx.Graph()
        md = dict(k="B")
        G.nx.add_edge("A", "B", **md)
        nxG.add_edge("A", "B", **md)
        assert dict(nx.bfs_successors(G.nx, "A")) == dict(nx.bfs_successors(nxG, "A"))
        G.nx.add_edge("B", "C", **md)
        nxG.add_edge("B", "C", **md)
        assert dict(nx.bfs_successors(G.nx, "A")) == dict(nx.bfs_successors(nxG, "A"))
        G.nx.add_edge("B", "D", **md)
        nxG.add_edge("B", "D", **md)
        assert dict(nx.bfs_successors(G.nx, "A")) == dict(nx.bfs_successors(nxG, "A"))
        assert dict(nx.bfs_successors(G.nx, "C")) == dict(nx.bfs_successors(nxG, "C"))

    def test_can_traverse_directed_graph(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=True, **kwargs))
        nxG = nx.DiGraph()
        md = dict(k="B")
        G.nx.add_edge("A", "B", **md)
        nxG.add_edge("A", "B", **md)
        assert dict(nx.bfs_successors(G.nx, "A")) == dict(nx.bfs_successors(nxG, "A"))
        G.nx.add_edge("B", "C", **md)
        nxG.add_edge("B", "C", **md)
        assert dict(nx.bfs_successors(G.nx, "A")) == dict(nx.bfs_successors(nxG, "A"))
        G.nx.add_edge("B", "D", **md)
        nxG.add_edge("B", "D", **md)
        assert dict(nx.bfs_successors(G.nx, "A")) == dict(nx.bfs_successors(nxG, "A"))
        assert dict(nx.bfs_successors(G.nx, "C")) == dict(nx.bfs_successors(nxG, "C"))

    def test_subgraph_isomorphism_undirected(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=False, **kwargs))
        nxG = nx.Graph()

        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        G.nx.add_edge("B", "C")
        nxG.add_edge("B", "C")
        G.nx.add_edge("C", "A")
        nxG.add_edge("C", "A")

        from networkx.algorithms.isomorphism import GraphMatcher

        assert len(
            [i for i in GraphMatcher(G.nx, G.nx).subgraph_monomorphisms_iter()]
        ) == len([i for i in GraphMatcher(nxG, nxG).subgraph_monomorphisms_iter()])

    def test_subgraph_isomorphism_directed(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=True, **kwargs))
        nxG = nx.DiGraph()

        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        G.nx.add_edge("B", "C")
        nxG.add_edge("B", "C")
        G.nx.add_edge("C", "A")
        nxG.add_edge("C", "A")

        from networkx.algorithms.isomorphism import DiGraphMatcher

        assert len(
            [i for i in DiGraphMatcher(G.nx, G.nx).subgraph_monomorphisms_iter()]
        ) == len([i for i in DiGraphMatcher(nxG, nxG).subgraph_monomorphisms_iter()])

    def test_can_get_edge_metadata(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        G.nx.add_edge("foo", "bar", baz=True)
        assert list(G.nx.edges(data=True)) == [("foo", "bar", {"baz": True})]

    def test_can_get_edges(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        G.nx.add_edge("foo", "bar", baz=True)
        assert list(G.backend.all_edges_as_iterable()) == [("foo", "bar")]

    def test_edge_dne_raises(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        G.nx.add_edge("foo", "bar", baz=True)

        assert G.nx.has_edge("foo", "crab") == False
        assert G.nx.has_edge("foo", "bar") == True
        # assert G.nx.edges[("foo", "bar")] != None
        # with pytest.raises(Exception):
        #     G.nx.edges[("foo", "crab")]

    def test_reverse_edges_in_undirected(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=False, **kwargs))
        G.nx.add_edge("foo", "bar", baz=True)

        assert G.nx.has_edge("foo", "bar") == True
        assert G.nx.has_edge("bar", "foo") == True

    def test_undirected_degree(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=False, **kwargs))
        G.nx.add_edge("foo", "bar", baz=True)
        assert G.nx.degree("foo") == 1
        assert G.nx.degree("bar") == 1

    def test_directed_degree(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=True, **kwargs))
        G.nx.add_edge("foo", "bar", baz=True)
        assert G.nx.degree("foo") == 1
        assert G.nx.degree("bar") == 0

    def test_undirected_degree_multiple(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=False, **kwargs))
        G.nx.add_edge("foo", "bar", baz=True)
        G.nx.add_edge("foo", "baz", baz=True)
        assert G.nx.degree("foo") == 2
        assert G.nx.degree("bar") == 1
        assert G.nx.degree("baz") == 1

    def test_directed_degree_multiple(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(directed=True, **kwargs))
        G.nx.add_edge("foo", "bar", baz=True)
        G.nx.add_edge("foo", "baz", baz=True)
        assert G.nx.degree("foo") == 2
        assert G.nx.degree("bar") == 0
        assert G.nx.degree("baz") == 0
        assert G.nx.out_degree("foo") == 2
        assert G.nx.out_degree("bar") == 0
        assert G.nx.out_degree("baz") == 0
        assert G.nx.in_degree("foo") == 0
        assert G.nx.in_degree("bar") == 1
        assert G.nx.in_degree("baz") == 1

    def test_node_count(self, backend):
        backend, kwargs = backend
        G = Graph(backend=backend(**kwargs))
        G.nx.add_node("foo", bar=True)
        G.nx.add_node("bar", foo=True)
        assert len(G.nx) == 2


@pytest.mark.benchmark
@pytest.mark.parametrize("backend", backend_test_params)
def test_node_addition_performance(backend):
    backend, kwargs = backend
    G = Graph(backend=backend(directed=True, **kwargs))
    for i in range(1000):
        G.nx.add_node(i)
    assert len(G.nx) == 1000


@pytest.mark.benchmark
@pytest.mark.parametrize("backend", backend_test_params)
def test_get_density_performance(backend):
    backend, kwargs = backend
    G = Graph(backend=backend(directed=True, **kwargs))
    for i in range(1000):
        G.nx.add_node(i)
    for i in range(1000 - 1):
        G.nx.add_edge(i, i + 1)
    assert nx.density(G.nx) <= 0.005
