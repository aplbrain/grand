import abc
from typing import Hashable, Generator

import networkx as nx


class Backend(abc.ABC):
    def __init__(self, directed: bool = False):
        ...

    def add_node(self, node_name: Hashable, metadata: dict):
        ...

    def get_node_by_id(self, node_name: Hashable):
        ...

    def all_nodes_as_generator(self, include_metadata: bool = False) -> Generator:
        ...

    def add_edge(self, u: Hashable, v: Hashable, metadata: dict):
        ...

    def all_edges_as_generator(self, include_metadata: bool = False) -> Generator:
        ...

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        ...


class NetworkXBackend(Backend):
    def __init__(self, directed: bool = False):
        self._nx_graph = nx.DiGraph() if directed else nx.Graph()

    def add_node(self, node_name: Hashable, metadata: dict):
        self._nx_graph.add_node(node_name, **metadata)

    def get_node_by_id(self, node_name: Hashable):
        return self._nx_graph.nodes[node_name]

    def all_nodes_as_generator(self, include_metadata: bool = False) -> Generator:
        return self._nx_graph.nodes(data=include_metadata)

    def add_edge(self, u: Hashable, v: Hashable, metadata: dict):
        self._nx_graph.add_edge(u, v, **metadata)

    def all_edges_as_generator(self, include_metadata: bool = False) -> Generator:
        return self._nx_graph.edges(data=include_metadata)

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        return self._nx_graph.edges[u, v]


_DEFAULT_BACKEND = NetworkXBackend


class Graph:
    def __init__(self, backend: Backend = None):
        self.backend = backend or _DEFAULT_BACKEND()

        class NetworkXAdapter:
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

        self.nx = NetworkXAdapter(self)
