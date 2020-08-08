import unittest

from . import Graph, NetworkXBackend


class TestGraph(unittest.TestCase):
    def test_can_create(self):
        Graph()

    def test_can_use_nx_backend(self):
        Graph().nx

    def test_can_add_node(self):
        G = Graph()
        G.nx.add_node("A", k="v")
        self.assertEqual(len(G.nx.nodes()), 1)
        G.nx.add_node("B", k="v")
        self.assertEqual(len(G.nx.nodes()), 2)

    def test_can_add_edge(self):
        G = Graph()
        G.nx.add_edge("A", "B")
        self.assertEqual(len(G.nx.edges()), 1)
        G.nx.add_edge("A", "B")
        self.assertEqual(len(G.nx.edges()), 1)

    def test_can_get_node(self):
        G = Graph()
        md = dict(k="B")
        G.nx.add_node("A", **md)
        self.assertEqual(G.nx["A"], md)

    def test_can_get_edge(self):
        G = Graph()
        md = dict(k="B")
        G.nx.add_edge("A", "B", **md)
        self.assertEqual(G.nx["A", "B"], md)

