from typing import Hashable, Generator, List, Tuple

import networkx as nx
from networkx.classes.reportviews import NodeView


class NetworkXDialect(nx.Graph):
    """
    A NetworkXDialect provides a networkx-like interface for graph manipulation

    """

    def __init__(self, parent: "Graph"):
        self.parent = parent

    def add_node(self, name: Hashable, **kwargs):
        return self.parent.backend.add_node(name, kwargs)

    # @property
    # def nodes(self):
    #     return NodeView(self)

    def add_edge(self, u: Hashable, v: Hashable, **kwargs):
        return self.parent.backend.add_edge(u, v, kwargs)

    def edges(self, data: bool = False):
        return self.parent.backend.all_edges_as_generator(include_metadata=data)

    def neighbors(self, u: Hashable) -> Generator:
        return self.parent.backend.get_node_neighbors(u)

    @property
    def _node(self):
        return {
            n: metadata
            for n, metadata in self.parent.backend.all_nodes_as_generator(
                include_metadata=True
            )
        }

    @property
    def _adj(self):
        # TODO: This is very inefficient for over-the-wire Backends.
        return {
            node: {
                neighbor: metadata
                for neighbor, metadata in self.parent.backend.get_node_neighbors(
                    node, include_metadata=True
                ).items()
            }
            for node in self.nodes()
        }
        # return self.nodes()

    # def __getitem__(self, key):
    #     if not isinstance(key, (tuple, list)):
    #         return self.parent.backend.get_node_by_id(key)
    #     return self.parent.backend.get_edge_by_id(*key)


class IGraphDialect(nx.Graph):
    """
    An IGraphDialect provides a python-igraph-like interface

    """

    def __init__(self, parent: "Graph"):
        self.parent = parent

    def add_vertices(self, num_verts: int):
        old_max = len(self.vs)
        for new_v_index in range(num_verts):
            return self.parent.backend.add_node(new_v_index + old_max, {})

    @property
    def vs(self):
        return [
            i for i in self.parent.backend.all_nodes_as_generator(include_metadata=True)
        ]

    @property
    def es(self):
        return [
            i for i in self.parent.backend.all_edges_as_generator(include_metadata=True)
        ]

    def add_edges(self, edgelist: List[Tuple[Hashable, Hashable]]):
        for (u, v) in edgelist:
            return self.parent.backend.add_edge(u, v, {})

    def get_edgelist(self):
        return self.parent.backend.all_edges_as_generator(include_metadata=False)


class CypherDialect:
    def __init__(self, parent: "Graph") -> None:
        self.parent = parent
        self._nxlike = NetworkXDialect(parent=parent)

    def query(self, query_text: str) -> any:
        """
        Perform a Cypher query on the current graph.

        Arguments:
            query_text (str): The cypher query text to run

        Returns:
            any

        """
        raise NotImplementedError()

