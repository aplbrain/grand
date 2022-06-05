import unittest

from . import Graph, DiGraph


class TestGraph(unittest.TestCase):
    def test_can_create(self):
        Graph()

    def test_can_use_nx_backend(self):
        Graph().nx

    def test_can_create_directed(self):
        assert Graph(directed=True).nx.is_directed() is True
        assert Graph(directed=False).nx.is_directed() is False
        assert DiGraph().nx.is_directed() is True
