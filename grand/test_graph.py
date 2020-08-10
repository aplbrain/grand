import unittest

from . import Graph, NetworkXBackend


class TestGraph(unittest.TestCase):
    def test_can_create(self):
        Graph()

    def test_can_use_nx_backend(self):
        Graph().nx

