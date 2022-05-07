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
    from ._sqlbackend import SQLBackend

    _CAN_IMPORT_SQL = True
except ImportError:
    _CAN_IMPORT_SQL = False

from .. import Graph


backend_test_params = [
    pytest.param(
        NetworkXBackend,
        marks=pytest.mark.skipif(
            os.environ.get("TEST_NETWORKXBACKEND", default="1") != "1",
            reason="NetworkX Backend skipped because $TEST_NETWORKXBACKEND != 1.",
        ),
    ),
]
backend_test_params = [
    pytest.param(
        DataFrameBackend,
        marks=pytest.mark.skipif(
            os.environ.get("TEST_DATAFRAMEBACKEND", default="1") != "1",
            reason="DataFrameBackend skipped because $TEST_DATAFRAMEBACKEND != 1.",
        ),
    ),
]

if _CAN_IMPORT_DYNAMODB:
    backend_test_params.append(
        pytest.param(
            DynamoDBBackend,
            marks=pytest.mark.skipif(
                os.environ.get("TEST_DYNAMODB", default="1") != "1",
                reason="DynamoDB Backend skipped because $TEST_DYNAMODB != 0 or boto3 is not installed",
            ),
        ),
    )

if _CAN_IMPORT_SQL:
    backend_test_params.append(
        pytest.param(
            SQLBackend,
            marks=pytest.mark.skipif(
                os.environ.get("TEST_SQLBACKEND", default="1") != "1"
                or not _CAN_IMPORT_SQL,
                reason="SQL Backend skipped because $TEST_SQLBACKEND != 1 or sqlalchemy is not installed.",
            ),
        ),
    )
if _CAN_IMPORT_IGRAPH:
    backend_test_params.append(
        pytest.param(
            IGraphBackend,
            marks=pytest.mark.skipif(
                os.environ.get("TEST_IGRAPBACKEND", default="1") != "1"
                or not _CAN_IMPORT_SQL,
                reason="IGraph Backend skipped because $TEST_IGRAPBACKEND != 1 or igraph is not installed.",
            ),
        ),
    )

if os.environ.get("TEST_NETWORKITBACKEND") == "1":
    from ._networkit import NetworkitBackend

    backend_test_params.append(
        pytest.param(
            NetworkitBackend,
            marks=pytest.mark.skipif(
                os.environ.get("TEST_NETWORKITBACKEND") != "1",
                reason="Networkit Backend skipped because $TEST_NETWORKITBACKEND != 1.",
            ),
        ),
    )

if os.environ.get("TEST_IGRAPHBACKEND") == "1":
    from ._igraph import IGraphBackend

    backend_test_params.append(
        pytest.param(
            IGraphBackend,
            marks=pytest.mark.skipif(
                os.environ.get("TEST_IGRAPHBACKEND") != "1",
                reason="Networkit Backend skipped because $TEST_IGRAPHBACKEND != 1.",
            ),
        ),
    )


@pytest.mark.parametrize("backend", backend_test_params)
class TestBackend:
    def test_can_create(self, backend):
        backend()

    def test_can_create_directed_and_undirected_backends(self, backend):
        b = backend(directed=True)
        assert b.is_directed() == True

        b = backend(directed=False)
        assert b.is_directed() == False

    def test_can_add_node(self, backend):
        G = Graph(backend=backend())
        nxG = nx.Graph()
        G.nx.add_node("A", k="v")
        nxG.add_node("A", k="v")
        assert len(G.nx.nodes()) == len(nxG.nodes())
        G.nx.add_node("B", k="v")
        nxG.add_node("B", k="v")
        assert len(G.nx.nodes()) == len(nxG.nodes())

    def test_can_update_node(self, backend):
        G = Graph(backend=backend())
        G.nx.add_node("A", k="v", z=3)
        G.nx.add_node("A", k="v2", x=4)
        assert G.nx.nodes["A"]["k"] == "v2"
        assert G.nx.nodes["A"]["x"] == 4
        assert G.nx.nodes["A"]["z"] == 3

    def test_can_add_edge(self, backend):
        G = Graph(backend=backend())
        nxG = nx.Graph()
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert len(G.nx.edges()) == len(nxG.edges())
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert len(G.nx.edges()) == len(nxG.edges())

    def test_can_update_edge(self, backend):
        G = Graph(backend=backend())
        G.nx.add_edge("A", "B", k="v", z=3)
        G.nx.add_edge("A", "B", k="v2", x=4)
        assert G.nx.get_edge_data("A", "B")["k"] == "v2"
        assert G.nx.get_edge_data("A", "B")["x"] == 4
        assert G.nx.get_edge_data("A", "B")["z"] == 3
        assert len(G.nx.nodes()) == 2

    def test_can_get_node(self, backend):
        G = Graph(backend=backend())
        nxG = nx.Graph()
        md = dict(k="B")
        G.nx.add_node("A", **md)
        nxG.add_node("A", **md)
        assert G.nx.nodes["A"] == nxG.nodes["A"]

    def test_can_get_edge(self, backend):
        G = Graph(backend=backend())
        nxG = nx.Graph()
        md = {"k": "B"}
        G.nx.add_edge("A", "B", **md)
        nxG.add_edge("A", "B", **md)
        assert G.nx.get_edge_data("A", "B") == nxG.get_edge_data("A", "B")

    def test_can_get_neighbors(self, backend):
        G = Graph(backend=backend())
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
        G = Graph(backend=backend())
        nxG = nx.Graph()
        assert G.nx._adj == nxG._adj
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert G.nx._adj == nxG._adj

    def test_directed_adj(self, backend):
        G = Graph(backend=backend(directed=True))
        nxG = nx.DiGraph()
        assert G.nx._adj == nxG._adj
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert G.nx._adj == nxG._adj

    def test_can_traverse_undirected_graph(self, backend):
        G = Graph(backend=backend())
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
        G = Graph(backend=backend(directed=True))
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
        G = Graph(backend=backend(directed=False))
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
        G = Graph(backend=backend(directed=True))
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
        G = Graph(backend=backend())
        G.nx.add_edge("foo", "bar", baz=True)
        assert list(G.nx.edges(data=True)) == [("foo", "bar", {"baz": True})]

    def test_edge_dne_raises(self, backend):
        G = Graph(backend=backend())
        G.nx.add_edge("foo", "bar", baz=True)

        assert G.nx.has_edge("foo", "crab") == False
        assert G.nx.has_edge("foo", "bar") == True
        # assert G.nx.edges[("foo", "bar")] != None
        # with pytest.raises(Exception):
        #     G.nx.edges[("foo", "crab")]

    def test_reverse_edges_in_undirected(self, backend):
        G = Graph(backend=backend(directed=False), directed=False)
        G.nx.add_edge("foo", "bar", baz=True)

        assert G.nx.has_edge("foo", "bar") == True
        assert G.nx.has_edge("bar", "foo") == True

    def test_undirected_degree(self, backend):
        G = Graph(backend=backend(directed=False))
        G.nx.add_edge("foo", "bar", baz=True)
        assert G.nx.degree("foo") == 1
        assert G.nx.degree("bar") == 1

    def test_directed_degree(self, backend):
        G = Graph(backend=backend(directed=True))
        G.nx.add_edge("foo", "bar", baz=True)
        assert G.nx.degree("foo") == 1
        assert G.nx.degree("bar") == 0

    def test_undirected_degree_multiple(self, backend):
        G = Graph(backend=backend(directed=False))
        G.nx.add_edge("foo", "bar", baz=True)
        G.nx.add_edge("foo", "baz", baz=True)
        assert G.nx.degree("foo") == 2
        assert G.nx.degree("bar") == 1
        assert G.nx.degree("baz") == 1

    def test_directed_degree_multiple(self, backend):
        G = Graph(backend=backend(directed=True))
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
