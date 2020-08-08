import unittest

from . import NetworkXBackend


class TestGraph(unittest.TestCase):
    def test_can_create(self):
        NetworkXBackend()

    def test_can_add_node(self):
        backend = NetworkXBackend()
        backend.add_node("A", dict(k="v"))
