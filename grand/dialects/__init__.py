from typing import Hashable, Generator, List, Tuple, Union
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .. import Graph

import pandas as pd

import networkx as nx
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
        return iter(self._parent.backend.all_nodes_as_iterable(include_metadata=False))

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
        return iter(self.parent.backend.all_nodes_as_iterable(include_metadata=False))

    def copy(self):
        return {
            n: metadata
            for n, metadata in self.parent.backend.all_nodes_as_iterable(
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

    def add_edge(self, u: Hashable, v: Hashable, **kwargs):
        return self.parent.backend.add_edge(u, v, kwargs)

    def remove_node(self, name: Hashable):
        if hasattr(self.parent.backend, "remove_node"):
            return self.parent.backend.remove_node(name)
        raise NotImplementedError

    def remove_edge(self, u: Hashable, v: Hashable):
        raise NotImplementedError

    def edges(self, data: bool = False):
        return [
            i for i in self.parent.backend.all_edges_as_iterable(include_metadata=data)
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
            i for i in self.parent.backend.all_nodes_as_iterable(include_metadata=True)
        ]

    @property
    def es(self):
        return [
            i for i in self.parent.backend.all_edges_as_iterable(include_metadata=True)
        ]

    def add_edges(self, edgelist: List[Tuple[Hashable, Hashable]]):
        for (u, v) in edgelist:
            return self.parent.backend.add_edge(u, v, {})

    def get_edgelist(self):
        return self.parent.backend.all_edges_as_iterable(include_metadata=False)


class NetworkitDialect:
    """
    A Networkit-like API for interacting with a Grand graph.

    For more details on the original API, see here:
    https://networkit.github.io/dev-docs/python_api/graph.html

    """

    def __init__(self, parent: "Graph") -> None:
        self.parent = parent

    def addNode(self):
        new_id = self.parent.backend.get_node_count()
        self.parent.backend.add_node(new_id)
        return new_id

    def addEdge(self, u: Hashable, v: Hashable) -> None:
        self.parent.backend.add_edge(u, v, {})

    def nodes(self):
        return [i for i in self.iterNodes()]

    def iterNodes(self):
        return self.parent.backend.all_nodes_as_iterable()

    def edges(self):
        return [i for i in self.iterEdges()]

    def iterEdges(self):
        return self.parent.backend.all_edges_as_iterable()

    def hasEdge(self, u, v) -> bool:
        return self.parent.backend.get_edge_by_id(u, v) is not None

    def addNodes(self, numberOfNewNodes: int) -> int:
        for _ in range(numberOfNewNodes):
            r = self.addNode()
        return r

    def hasNode(self, u) -> bool:
        return self.parent.backend.has_node(u)

    def degree(self, v):
        return len(self.parent.backend.get_node_neighbors(v))

    def degreeIn(self, v):
        return len(self.parent.backend.get_node_predecessors(v))

    def degreeOut(self, v):
        return len(self.parent.backend.get_node_successors(v))

    def density(self):
        # TODO: implement backend#degree?
        E = len(self.parent.backend.all_edges_as_iterable())
        V = self.parent.backend.get_node_count()

        if self.parent.backend.is_directed():
            return E / (V * (V - 1))
        else:
            return 2 * E / (V * (V - 1))

    def numberOfNodes(self) -> int:
        return self.parent.backend.get_node_count()

    def numberOfEdges(self) -> int:
        return len(self.parent.backend.all_edges_as_iterable())

    def removeEdge(self, u, v) -> None:
        raise NotImplementedError
        return self.parent.backend.remove_edge(u, v)

    def removeNode(self, u: Hashable) -> None:
        if hasattr(self.parent.backend, "remove_node"):
            return self.parent.backend.remove_node(u)
        raise NotImplementedError


    def append(self, G):
        raise NotImplementedError

    def copyNodes(self):
        raise NotImplementedError

    def BFSEdgesFrom(self, start: Union[int, List[int]]):
        raise NotImplementedError


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
