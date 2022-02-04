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


# class TestDotMotifDialect(unittest.TestCase):
#     def test_dm_monomorphism_undirected(self):
#         G = Graph(directed=False)
#         G.nx.add_edge("1", "2")
#         G.nx.add_edge("2", "3")
#         G.nx.add_edge("3", "1")

#         self.assertEqual(
#             len(G.dm.find("""A -> B\nB -> C\nC -> A""")), 6,
#         )

#     def test_dm_monomorphism_directed(self):
#         G = Graph(directed=True)
#         G.nx.add_edge("1", "2")
#         G.nx.add_edge("2", "3")
#         G.nx.add_edge("3", "1")

#         self.assertEqual(
#             len(G.dm.find("""A -> B\nB -> C\nC -> A""")), 3,
#         )

#     def test_dm_monomorphism_undirected_automorphisms(self):
#         G = Graph(directed=False)
#         G.nx.add_edge("1", "2")
#         G.nx.add_edge("2", "3")
#         G.nx.add_edge("3", "1")
#         G.nx.add_edge("3", "4")

#         self.assertEqual(
#             len(G.dm.find("""A -> B\nB -> C\nC -> A""", exclude_automorphisms=True)), 1,
#         )

#     def test_count_dm_monomorphism_undirected(self):
#         G = Graph(directed=False)
#         G.nx.add_edge("1", "2")
#         G.nx.add_edge("2", "3")
#         G.nx.add_edge("3", "1")

#         self.assertEqual(
#             G.dm.count("""A -> B\nB -> C\nC -> A"""), 6,
#         )

#     def test_count_dm_monomorphism_directed(self):
#         G = Graph(directed=True)
#         G.nx.add_edge("1", "2")
#         G.nx.add_edge("2", "3")
#         G.nx.add_edge("3", "1")

#         self.assertEqual(
#             G.dm.count("""A -> B\nB -> C\nC -> A"""), 3,
#         )

#     def test_count_dm_monomorphism_undirected_automorphisms(self):
#         G = Graph(directed=False)
#         G.nx.add_edge("1", "2")
#         G.nx.add_edge("2", "3")
#         G.nx.add_edge("3", "1")
#         G.nx.add_edge("3", "4")

#         self.assertEqual(
#             G.dm.count("""A -> B\nB -> C\nC -> A""", exclude_automorphisms=True), 1,
#         )
