import io
import unittest

from .. import Graph
from . import (
    NetworkXDialect,
    IGraphDialect,
    NetworkitDialect,
    _GrandAdjacencyView,
    _GrandNodeAtlasView,
)
import networkx as nx


class TestIGraphDialect(unittest.TestCase):
    def test_igraph_verts(self):
        G = Graph()
        self.assertEqual(G.igraph.vs, [])
        G.nx.add_node("1")
        self.assertEqual(G.igraph.vs, [("1", {})])

    def test_add_verts(self):
        G = Graph()
        G.igraph.add_vertices(1)
        self.assertEqual(G.igraph.vs, [(0, {})])
        G.igraph.add_vertices(1)
        self.assertEqual(G.igraph.vs, [(0, {}), (1, {})])
        G.igraph.add_vertices(10)
        self.assertEqual(len(G.igraph.vs), 12)

    def test_igraph_edges(self):
        G = Graph()
        G.igraph.add_vertices(2)
        G.igraph.add_edges([(0, 1)])
        self.assertEqual(G.igraph.es, [(0, 1, {})])


class TestNetworkXHelpers(unittest.TestCase):
    def test_nx_adj_length(self):
        G = Graph()
        assert len(G.nx.adj) == 0
        G.nx.add_node("1")
        G.nx.add_edge("1", "2")
        assert len(G.nx.adj["1"]) == 1


class TestNetworkXDialect(unittest.TestCase):
    def test_nx_pred(self):
        G = Graph(directed=True)
        G.nx.add_edge("1", "2")
        G.nx.add_edge("1", "3")
        H = nx.DiGraph()
        H.add_edge("1", "2")
        H.add_edge("1", "3")
        self.assertEqual(G.nx.pred, H.pred)

    def test_nx_directed(self):
        G = Graph(directed=True)
        self.assertTrue(G.nx.is_directed())

        G = Graph(directed=False)
        self.assertFalse(G.nx.is_directed())

    def test_in_degree(self):
        G = Graph(directed=True)
        G.nx.add_edge("1", "2")
        G.nx.add_edge("1", "3")
        H = nx.DiGraph()
        H.add_edge("1", "2")
        H.add_edge("1", "3")
        self.assertEqual(dict(G.nx.in_degree()), dict(H.in_degree()))

    def test_out_degree(self):
        G = Graph(directed=True)
        G.nx.add_edge("1", "2")
        G.nx.add_edge("1", "3")
        H = nx.DiGraph()
        H.add_edge("1", "2")
        H.add_edge("1", "3")
        self.assertEqual(dict(G.nx.out_degree()), dict(H.out_degree()))

    def test_nx_edges(self):
        G = Graph(directed=True).nx
        H = nx.DiGraph()
        G.add_edge("1", "2")
        G.add_edge("1", "3")
        H.add_edge("1", "2")
        H.add_edge("1", "3")
        self.assertEqual(dict(G.edges), dict(H.edges))
        self.assertEqual(dict(G.edges()), dict(H.edges()))
        self.assertEqual(list(G.edges["1", "2"]), list(H.edges["1", "2"]))

    def test_nx_export(self):
        gg = Graph()
        f = io.BytesIO()
        nx.write_graphml(gg.nx, f)


class TestNetworkitDialect(unittest.TestCase):
    def test_add_verts(self):
        G = Graph()
        u = G.networkit.addNode()
        assert G.networkit.hasNode(u)
        assert not G.networkit.hasNode("X")

    def test_add_edges(self):
        G = Graph()
        u = G.networkit.addNode()
        v = G.networkit.addNode()
        assert len(G.networkit.edges()) == 0
        assert G.networkit.numberOfEdges() == 0
        G.networkit.addEdge(u, v)
        assert G.networkit.hasEdge(u, v)
        assert len(G.networkit.edges()) == 1
        assert G.networkit.numberOfEdges() == 1
        assert G.networkit.numberOfNodes() == 2

    def test_nodes(self):
        G = Graph()
        G.networkit.addNode()
        G.networkit.addNode()
        self.assertEqual(len(G.networkit.nodes()), 2)
        assert G.networkit.numberOfNodes() == 2

    def test_undirected_degree(self):
        G = Graph(directed=False)
        assert G.networkit.numberOfNodes() == 0
        assert G.networkit.numberOfEdges() == 0
        u = G.networkit.addNode()
        v = G.networkit.addNode()
        assert G.networkit.numberOfNodes() == 2
        assert G.networkit.numberOfEdges() == 0
        assert G.networkit.degree(u) == 0
        assert G.networkit.degree(v) == 0
        G.networkit.addEdge(u, v)
        assert G.networkit.degree(u) == 1
        assert G.networkit.degree(v) == 1
        assert G.networkit.numberOfEdges() == 1

    def test_directed_degree(self):
        G = Graph(directed=True)
        assert G.networkit.numberOfNodes() == 0
        assert G.networkit.numberOfEdges() == 0
        u = G.networkit.addNode()
        v = G.networkit.addNode()
        assert G.networkit.numberOfNodes() == 2
        assert G.networkit.numberOfEdges() == 0
        assert G.networkit.degreeIn(u) == 0
        assert G.networkit.degreeOut(u) == 0
        assert G.networkit.degreeIn(v) == 0
        assert G.networkit.degreeOut(v) == 0
        G.networkit.addEdge(u, v)
        assert G.networkit.degreeIn(u) == 0
        assert G.networkit.degreeOut(u) == 1
        assert G.networkit.degreeIn(v) == 1
        assert G.networkit.degreeOut(v) == 0
        assert G.networkit.numberOfEdges() == 1

    def test_undirected_density(self):
        G = Graph(directed=False)
        u = G.networkit.addNode()
        v = G.networkit.addNode()
        G.networkit.addEdge(u, v)
        assert G.networkit.density() == 1

    def test_directed_density(self):
        G = Graph(directed=True)
        u = G.networkit.addNode()
        v = G.networkit.addNode()
        G.networkit.addEdge(u, v)
        assert G.networkit.density() == 0.5
        G.networkit.addEdge(v, u)
        assert G.networkit.density() == 1
