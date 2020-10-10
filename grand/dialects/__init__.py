from typing import Hashable, Generator, List, Tuple, Union
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .. import Graph

import networkx as nx
import pandas as pd

from networkx.classes.reportviews import NodeView
from networkx.classes.coreviews import AdjacencyView, AtlasView


class _GrandAdjacencyView(AdjacencyView):

    __slots__ = ("_parent", "_pred_or_succ")  # Still uses AtlasView slots names _atlas

    def __init__(self, parent_nx_dialect: "NetworkXDialect", pred_or_succ: str):
        self._parent = parent_nx_dialect.parent
        self._pred_or_succ = pred_or_succ

    def __getitem__(self, name):
        if self._pred_or_succ == "pred":
            return {
                neighbor: metadata
                for neighbor, metadata in self._parent.backend.get_node_predecessors(
                    name, include_metadata=True
                ).items()
            }
        elif self._pred_or_succ == "succ":
            return {
                neighbor: metadata
                for neighbor, metadata in self._parent.backend.get_node_successors(
                    name, include_metadata=True
                ).items()
            }

    def __len__(self):
        return len(self._parent.backend.get_node_count())

    def __iter__(self):
        return iter(self._parent.backend.all_nodes_as_generator(include_metadata=False))

    def copy(self):
        raise NotImplementedError()

    def __str__(self):
        return "_GrandAdjacencyView"

    def __repr__(self):
        return "_GrandAdjacencyView"


class _GrandNodeAtlasView(AtlasView):
    def __init__(self, parent):
        self.parent = parent.parent

    def __getitem__(self, key):
        return self.parent.backend.get_node_by_id(key)

    def __len__(self):
        return self.parent.backend.get_node_count()

    def __iter__(self):
        return iter(self.parent.backend.all_nodes_as_generator(include_metadata=False))

    def copy(self):
        return {
            n: metadata
            for n, metadata in self.parent.backend.all_nodes_as_generator(
                include_metadata=True
            )
        }

    def __str__(self):
        return "_GrandNodeAtlasView"

    def __repr__(self):
        return "_GrandNodeAtlasView"


class NetworkXDialect(nx.Graph):
    """
    A NetworkXDialect provides a networkx-like interface for graph manipulation

    """

    def __init__(self, parent: "Graph"):
        """
        Create a new dialect to query a backend with NetworkX syntax.

        Arguments:
            parent (Graph): The parent Graph object

        Returns:
            None

        """
        self.parent = parent

    def add_node(self, name: Hashable, **kwargs):
        return self.parent.backend.add_node(name, kwargs)

    # @property
    # def nodes(self)
    #     return NodeView(self)

    def add_edge(self, u: Hashable, v: Hashable, **kwargs):
        return self.parent.backend.add_edge(u, v, kwargs)

    def edges(self, data: bool = False):
        return [
            i for i in self.parent.backend.all_edges_as_generator(include_metadata=data)
        ]

    def neighbors(self, u: Hashable) -> Generator:
        return self.parent.backend.get_node_neighbors(u)

    def predecessors(self, u: Hashable) -> Generator:
        return self.parent.backend.get_node_predecessors(u)

    def successors(self, u: Hashable) -> Generator:
        return self.parent.backend.get_node_neighbors(u)

    @property
    def _node(self):
        return _GrandNodeAtlasView(self)

    @property
    def adj(self):
        """
        https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323
        """
        return _GrandAdjacencyView(self, "succ")

    @property
    def _adj(self):
        """
        https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323
        """
        return _GrandAdjacencyView(self, "succ")

    @property
    def succ(self):
        return _GrandAdjacencyView(self, "succ")

    @property
    def _succ(self):
        return _GrandAdjacencyView(self, "succ")

    @property
    def pred(self):
        """
        https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323
        """
        return _GrandAdjacencyView(self, "pred")

    @property
    def _pred(self):
        """
        https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323
        """
        return _GrandAdjacencyView(self, "pred")


class IGraphDialect(nx.Graph):
    """
    An IGraphDialect provides a python-igraph-like interface

    """

    def __init__(self, parent: "Graph"):
        """
        Create a new dialect to query a backend with Python-IGraph syntax.

        Arguments:
            parent (Graph): The parent Graph object

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
#             parent (Graph): The parent Graph object

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
