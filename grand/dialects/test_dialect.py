import unittest

from .. import Graph
import networkx as nx


class TestIGraphDialect(unittest.TestCase):
    def test_igraph_verts(self):
        G = Graph()
        self.assertEqual(G.igraph.vs, [])
        G.nx.add_node("1")
        self.assertEqual(G.igraph.vs, [("1", {})])


class TestNetworkXDialect(unittest.TestCase):
    def test_nx_pred(self):
        G = Graph(directed=True)
        G.nx.add_edge("1", "2")
        G.nx.add_edge("1", "3")
        H = nx.DiGraph()
        H.add_edge("1", "2")
        H.add_edge("1", "3")
        self.assertEqual(G.nx.pred, H.pred)

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
