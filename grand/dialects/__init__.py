from typing import Hashable, Generator, List, Tuple, Union

import networkx as nx
import pandas as pd

from networkx.classes.reportviews import NodeView


class NetworkXDialect(nx.Graph):
    """
    A NetworkXDialect provides a networkx-like interface for graph manipulation

    """

    def __init__(self, parent: "Graph"):
        """
        Create a new dialect to query a backend with NetworkX syntax.

        Arguments:
            parent (Graph): The parent grand.Graph object

        Returns:
            None

        """
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

    def predecessors(self, u: Hashable) -> Generator:
        return self.parent.backend.get_node_predecessors(u)

    def successors(self, u: Hashable) -> Generator:
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

    @property
    def pred(self):
        # TODO: This is very inefficient for over-the-wire Backends.
        return {
            node: {
                neighbor: metadata
                for neighbor, metadata in self.parent.backend.get_node_predecessors(
                    node, include_metadata=True
                ).items()
            }
            for node in self.nodes()
        }


class IGraphDialect(nx.Graph):
    """
    An IGraphDialect provides a python-igraph-like interface

    """

    def __init__(self, parent: "Graph"):
        """
        Create a new dialect to query a backend with Python-IGraph syntax.

        Arguments:
            parent (Graph): The parent grand.Graph object

        Returns:
            None

        """
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


# from dotmotif import dotmotif, NetworkXExecutor
#
# class DotMotifDialect:
#     def __init__(self, parent: "Graph") -> None:
#         """
#         Create a new DotMotifDialect to query a backend with DotMotif syntax.

#         Arguments:
#             parent (Graph): The parent grand.Graph object

#         Returns:
#             None

#         """
#         self.parent = parent

#     def find(
#         self, motif: Union[str, dotmotif], exclude_automorphisms: bool = False
#     ) -> pd.DataFrame:
#         """
#         Find a motif using DotMotif syntax.

#         Arguments:
#             motif (Union[str, dotmotif.dotmotif]): A motif in dotmotif form or
#                 a string in the DotMotif DSL.
#             exclude_automorphisms (bool: True): Whether to exclude motif
#                 automorphisms from the results list

#         Returns:
#             pd.DataFrame: A DataFrame containing the results of the query

#         """
#         if isinstance(motif, str):
#             motif = dotmotif(
#                 ignore_direction=(not self.parent.backend._directed),
#                 exclude_automorphisms=exclude_automorphisms,
#             ).from_motif(motif)

#         return NetworkXExecutor(graph=self.parent.nx).find(motif)

#     def count(
#         self, motif: Union[str, dotmotif], exclude_automorphisms: bool = False
#     ) -> pd.DataFrame:
#         """
#         Count occurrences of a motif using DotMotif syntax.

#         Arguments:
#             motif (Union[str, dotmotif.dotmotif]): A motif in dotmotif form or
#                 a string in the DotMotif DSL.
#             exclude_automorphisms (bool: True): Whether to exclude motif
#                 automorphisms from the results list

#         Returns:
#             int: A count of results of the motif query

#         """
#         if isinstance(motif, str):
#             motif = dotmotif(
#                 ignore_direction=(not self.parent.backend._directed),
#                 exclude_automorphisms=exclude_automorphisms,
#             ).from_motif(motif)

#         return NetworkXExecutor(graph=self.parent.nx).count(motif)
