"""
Grand graph databasifier.

Aug 2020
"""

# docker run -p 8000:8000 amazon/dynamodb-local

import abc
from typing import Hashable, Generator

import networkx as nx

from .backends import Backend, NetworkXBackend


_DEFAULT_BACKEND = NetworkXBackend


class Graph:
    def __init__(self, backend: Backend = None):
        self.backend = backend or _DEFAULT_BACKEND()

        class _NetworkXAdapter(nx.Graph):
            # class _NetworkXAdapter(nx.Graph):
            def __init__(self, parent: "Graph"):
                self.parent = parent

            def add_node(self, name: Hashable, **kwargs):
                return self.parent.backend.add_node(name, kwargs)

            def nodes(self, data: bool = False):
                return self.parent.backend.all_nodes_as_generator(include_metadata=data)

            def add_edge(self, u: Hashable, v: Hashable, **kwargs):
                return self.parent.backend.add_edge(u, v, kwargs)

            def edges(self, data: bool = False):
                return self.parent.backend.all_edges_as_generator(include_metadata=data)

            def __getitem__(self, key):
                if not isinstance(key, (tuple, list)):
                    return self.parent.backend.get_node_by_id(key)
                return self.parent.backend.get_edge_by_id(*key)

        self.nx = _NetworkXAdapter(self)
