from typing import Hashable, Generator, Iterable

from igraph import Graph, InternalError
import pandas as pd

from .backend import Backend


def _remove_name_from_attributes(attributes: dict):
    attrs = attributes
    attrs.pop("name")
    return attrs


class IGraphBackend(Backend):
    """
    This is currently UNOPTIMIZED CODE.

    Recommendations for future work include improved indexing and caching of
    node names and metadata.

    """

    def __init__(self, directed: bool = False):
        """
        Create a new IGraphBackend instance, using an igraph.Graph object to
        store and manage network structure.

        Arguments:
            directed (bool: False): Whether to make the backend graph directed

        Returns:
            None

        """
        self._directed = directed
        self._ig = Graph(directed=self._directed)

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> None:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        raise NotImplementedError()

    def is_directed(self) -> bool:
        """
        Return True if the backend graph is directed.

        Arguments:
            None

        Returns:
            bool: True if the backend graph is directed.

        """
        return self._directed

    def add_node(self, node_name: Hashable, metadata: dict):
        """
        Add a new node to the graph.

        Arguments:
            node_name (Hashable): The ID of the node
            metadata (dict: None): An optional dictionary of metadata

        Returns:
            Hashable: The ID of this node, as inserted

        """
        # TODO: overwrite existing
        self._ig.add_vertex(node_name, **metadata)
        return node_name

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        return _remove_name_from_attributes(
            self._ig.vs.find(name=node_name).attributes()
        )

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        if include_metadata:
            for v in self._ig.vs:
                yield (v["name"], _remove_name_from_attributes(v.attributes()))
        else:
            for i in self._ig.vs:
                yield i["name"]

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        try:
            self._ig.vs.find(name=u)
            return True
        except:
            return False

    def add_edge(self, u: Hashable, v: Hashable, metadata: dict):
        """
        Add a new edge to the graph between two nodes.

        If the graph is directed, this edge will start (source) at the `u` node
        and end (target) at the `v` node.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID
            metadata (dict): Optional metadata to associate with the edge

        Returns:
            Hashable: The edge ID, as inserted.

        """
        if self.has_edge(u, v):
            # TODO: update metadata
            return
        if not self.has_node(u):
            self.add_node(u, {})
        if not self.has_node(v):
            self.add_node(v, {})
        return self._ig.add_edge(source=u, target=v, **metadata)

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        if include_metadata:
            for e in self._ig.es:
                yield (e.source_vertex["name"], e.target_vertex["name"], e.attributes())
        else:
            for e in self._ig.es:
                yield e.source_vertex["name"], e.target_vertex["name"]

    def has_edge(self, u, v):
        try:
            self._ig.get_eid(u, v)
            return True
        except (InternalError, ValueError):
            # InternalError means no such edge
            # ValueError means one vertex doesn't exist
            return False

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        try:
            return self._ig.es[self._ig.get_eid(u, v)].attributes()
        except InternalError:
            raise IndexError(f"The edge ({u}, {v}) is not in the graph.")

    def get_node_successors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Generator:
        return self.get_node_neighbors(u, include_metadata)

    def get_node_neighbors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Generator:
        """
        Get a generator of all downstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """
        if include_metadata:
            return {
                self._ig.vs[s]["name"]: self.get_edge_by_id(u, s)
                for s in self._ig.successors(u)
            }
        else:
            return iter([self._ig.vs[s]["name"] for s in self._ig.successors(u)])

    def get_node_predecessors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Generator:
        """
        Get a generator of all upstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """
        if include_metadata:
            return {
                self._ig.vs[s]["name"]: self.get_edge_by_id(s, u)
                for s in self._ig.predecessors(u)
            }
        else:
            return iter([self._ig.vs[s]["name"] for s in self._ig.predecessors(u)])

    def get_node_count(self) -> Iterable:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return self._ig.vcount()
