import pytest
import os

import networkx as nx

from . import NetworkXBackend, SQLBackend, DynamoDBBackend
from .. import Graph

backend_test_params = [
    pytest.param(
        NetworkXBackend,
        marks=pytest.mark.skipif(
            os.environ.get("TEST_NETWORKXBACKEND", default="1") != "1",
            reason="NetworkX Backend skipped because $TEST_NETWORKXBACKEND != 0.",
        ),
    ),
    pytest.param(
        SQLBackend,
        marks=pytest.mark.skipif(
            os.environ.get("TEST_SQLBACKEND", default="1") != "1",
            reason="SQL Backend skipped because $TEST_SQLBACKEND != 0.",
        ),
    ),
    pytest.param(
        DynamoDBBackend,
        marks=pytest.mark.skipif(
            os.environ.get("TEST_DYNAMODBBACKEND") != "1",
            reason="DynamoDB Backend skipped because $TEST_DYNAMODBBACKEND != 0.",
        ),
    ),
]

if os.environ.get("TEST_NETWORKITBACKEND") == "1":
    from .networkit import NetworkitBackend

    backend_test_params.append(
        pytest.param(
            NetworkitBackend,
            marks=pytest.mark.skipif(
                os.environ.get("TEST_NETWORKITBACKEND") != "1",
                reason="Networkit Backend skipped because $TEST_NETWORKITBACKEND != 0.",
            ),
        ),
    )

if os.environ.get("TEST_IGRAPHBACKEND") == "1":
    from .igraph import IGraphBackend

    backend_test_params.append(
        pytest.param(
            IGraphBackend,
            marks=pytest.mark.skipif(
                os.environ.get("TEST_IGRAPHBACKEND") != "1",
                reason="Networkit Backend skipped because $TEST_IGRAPHBACKEND != 0.",
            ),
        ),
    )


@pytest.mark.parametrize("backend", backend_test_params)
class TestBackend:
    def test_can_create(self, backend):
        backend()

    def test_can_add_node(self, backend):
        G = Graph(backend=backend())
        nxG = nx.Graph()
        G.nx.add_node("A", k="v")
        nxG.add_node("A", k="v")
        assert len(G.nx.nodes()) == len(nxG.nodes())
        G.nx.add_node("B", k="v")
        nxG.add_node("B", k="v")
        assert len(G.nx.nodes()) == len(nxG.nodes())

    def test_can_add_edge(self, backend):
        G = Graph(backend=backend())
        nxG = nx.Graph()
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert len(G.nx.edges()) == len(nxG.edges())
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        assert len(G.nx.edges()) == len(nxG.edges())

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
        md = {"k":"B"}
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
        G = Graph(backend=SQLBackend(directed=True))
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
        assert G.nx.edges(data=True) == [("foo", "bar", {"baz": True})]

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
