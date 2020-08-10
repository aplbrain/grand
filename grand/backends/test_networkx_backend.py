import unittest

import networkx as nx

from . import NetworkXBackend
from .. import Graph


class TestNetworkXBackend(unittest.TestCase):
    def test_can_create(self):
        NetworkXBackend()

    def setUp(self):
        pass

    # def tearDown(self):
    #     NetworkXBackend().teardown(yes_i_am_sure=True)

    def test_can_add_node(self):
        G = Graph(backend=NetworkXBackend())
        nxG = nx.Graph()
        G.nx.add_node("A", k="v")
        nxG.add_node("A", k="v")
        self.assertEqual(len(G.nx.nodes()), len(nxG.nodes()))
        G.nx.add_node("B", k="v")
        nxG.add_node("B", k="v")
        self.assertEqual(len(G.nx.nodes()), len(nxG.nodes()))

    def test_can_add_edge(self):
        G = Graph(backend=NetworkXBackend())
        nxG = nx.Graph()
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        self.assertEqual(len(G.nx.edges()), len(nxG.edges()))
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        self.assertEqual(len(G.nx.edges()), len(nxG.edges()))

    def test_can_get_node(self):
        G = Graph(backend=NetworkXBackend())
        nxG = nx.Graph()
        md = dict(k="B")
        G.nx.add_node("A", **md)
        nxG.add_node("A", **md)
        self.assertEqual(G.nx.nodes["A"], nxG.nodes["A"])

    def test_can_get_edge(self):
        G = Graph(backend=NetworkXBackend())
        nxG = nx.Graph()
        md = dict(k="B")
        G.nx.add_edge("A", "B", **md)
        nxG.add_edge("A", "B", **md)
        self.assertEqual(G.nx.get_edge_data("A", "B"), nxG.get_edge_data("A", "B"))

    def test_can_get_neighbors(self):
        G = Graph(backend=NetworkXBackend())
        nxG = nx.Graph()
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        self.assertEqual(
            sorted([i for i in G.nx.neighbors("A")]),
            sorted([i for i in nxG.neighbors("A")]),
        )
        self.assertEqual(
            sorted([i for i in G.nx.neighbors("B")]),
            sorted([i for i in nxG.neighbors("B")]),
        )
        G.nx.add_edge("A", "C")
        nxG.add_edge("A", "C")
        self.assertEqual(
            sorted([i for i in G.nx.neighbors("A")]),
            sorted([i for i in nxG.neighbors("A")]),
        )
        self.assertEqual(
            sorted([i for i in G.nx.neighbors("B")]),
            sorted([i for i in nxG.neighbors("B")]),
        )
        self.assertEqual(
            sorted([i for i in G.nx.neighbors("C")]),
            sorted([i for i in nxG.neighbors("C")]),
        )

    def test_undirected_adj(self):
        G = Graph(backend=NetworkXBackend())
        nxG = nx.Graph()
        self.assertEqual(G.nx._adj, nxG._adj)
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        self.assertEqual(G.nx._adj, nxG._adj)

    def test_directed_adj(self):
        G = Graph(backend=NetworkXBackend(directed=True))
        nxG = nx.DiGraph()
        self.assertEqual(G.nx._adj, nxG._adj)
        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        self.assertEqual(G.nx._adj, nxG._adj)

    def test_can_traverse_undirected_graph(self):
        G = Graph(backend=NetworkXBackend())
        nxG = nx.Graph()
        md = dict(k="B")
        G.nx.add_edge("A", "B", **md)
        nxG.add_edge("A", "B", **md)
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "A")), dict(nx.bfs_successors(nxG, "A"))
        )
        G.nx.add_edge("B", "C", **md)
        nxG.add_edge("B", "C", **md)
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "A")), dict(nx.bfs_successors(nxG, "A"))
        )
        G.nx.add_edge("B", "D", **md)
        nxG.add_edge("B", "D", **md)
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "A")), dict(nx.bfs_successors(nxG, "A"))
        )
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "C")), dict(nx.bfs_successors(nxG, "C"))
        )

    def test_can_traverse_directed_graph(self):
        G = Graph(backend=NetworkXBackend(directed=True))
        nxG = nx.DiGraph()
        md = dict(k="B")
        G.nx.add_edge("A", "B", **md)
        nxG.add_edge("A", "B", **md)
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "A")), dict(nx.bfs_successors(nxG, "A"))
        )
        G.nx.add_edge("B", "C", **md)
        nxG.add_edge("B", "C", **md)
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "A")), dict(nx.bfs_successors(nxG, "A"))
        )
        G.nx.add_edge("B", "D", **md)
        nxG.add_edge("B", "D", **md)
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "A")), dict(nx.bfs_successors(nxG, "A"))
        )
        self.assertEqual(
            dict(nx.bfs_successors(G.nx, "C")), dict(nx.bfs_successors(nxG, "C"))
        )

    def test_subgraph_isomorphism_undirected(self):
        G = Graph(backend=NetworkXBackend(directed=False))
        nxG = nx.Graph()

        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        G.nx.add_edge("B", "C")
        nxG.add_edge("B", "C")
        G.nx.add_edge("C", "A")
        nxG.add_edge("C", "A")

        from networkx.algorithms.isomorphism import GraphMatcher

        self.assertEqual(
            len([i for i in GraphMatcher(G.nx, G.nx).subgraph_monomorphisms_iter()]),
            len([i for i in GraphMatcher(nxG, nxG).subgraph_monomorphisms_iter()]),
        )

    def test_subgraph_isomorphism_directed(self):
        G = Graph(backend=NetworkXBackend(directed=True))
        nxG = nx.DiGraph()

        G.nx.add_edge("A", "B")
        nxG.add_edge("A", "B")
        G.nx.add_edge("B", "C")
        nxG.add_edge("B", "C")
        G.nx.add_edge("C", "A")
        nxG.add_edge("C", "A")

        from networkx.algorithms.isomorphism import DiGraphMatcher

        self.assertEqual(
            len([i for i in DiGraphMatcher(G.nx, G.nx).subgraph_monomorphisms_iter()]),
            len([i for i in DiGraphMatcher(nxG, nxG).subgraph_monomorphisms_iter()]),
        )
