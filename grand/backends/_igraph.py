from typing import Hashable, Collection

from igraph import Graph
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
        self._node_ids_by_name = {}
        self._edge_ids_by_key = {}

    def _edge_key(self, u: Hashable, v: Hashable):
        if self._directed:
            return (u, v)
        return frozenset((u, v))

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
        metadata = metadata or {}

        if self.has_node(node_name):
            # Update metadata
            self._ig.vs[self._node_ids_by_name[node_name]].update_attributes(metadata)
            return node_name

        self._ig.add_vertex(name=node_name, **metadata)
        self._node_ids_by_name[node_name] = self._ig.vcount() - 1
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
            self._ig.vs[self._node_ids_by_name[node_name]].attributes()
        )

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Collection:
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
        return u in self._node_ids_by_name

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
        metadata = metadata or {}
        edge_key = self._edge_key(u, v)

        if edge_key in self._edge_ids_by_key:
            self._ig.es[self._edge_ids_by_key[edge_key]].update_attributes(metadata)
            return
        if not self.has_node(u):
            self.add_node(u, {})
        if not self.has_node(v):
            self.add_node(v, {})

        new_edge = self._ig.add_edge(
            source=self._node_ids_by_name[u],
            target=self._node_ids_by_name[v],
            **metadata,
        )
        self._edge_ids_by_key[edge_key] = self._ig.ecount() - 1
        return new_edge

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Collection:
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
        return self._edge_key(u, v) in self._edge_ids_by_key

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        edge_id = self._edge_ids_by_key.get(self._edge_key(u, v))
        if edge_id is None:
            raise IndexError(f"The edge ({u}, {v}) is not in the graph.")
        return self._ig.es[edge_id].attributes()

    def get_node_successors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Collection:
        return self.get_node_neighbors(u, include_metadata)

    def get_node_neighbors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Collection:
        """
        Get a generator of all downstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """
        u_id = self._node_ids_by_name[u]

        if include_metadata:
            return {
                self._ig.vs[s]["name"]: self.get_edge_by_id(u, self._ig.vs[s]["name"])
                for s in self._ig.successors(u_id)
            }
        else:
            return iter([self._ig.vs[s]["name"] for s in self._ig.successors(u_id)])

    def get_node_predecessors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Collection:
        """
        Get a generator of all upstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """
        u_id = self._node_ids_by_name[u]

        if include_metadata:
            return {
                self._ig.vs[s]["name"]: self.get_edge_by_id(self._ig.vs[s]["name"], u)
                for s in self._ig.predecessors(u_id)
            }
        else:
            return iter([self._ig.vs[s]["name"] for s in self._ig.predecessors(u_id)])

    def get_node_count(self) -> int:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return self._ig.vcount()

    def get_edge_count(self) -> int:
        """
        Get an integer count of the number of edges in this graph.

        Arguments:
            None

        Returns:
            int: The count of edges

        """
        return self._ig.ecount()
