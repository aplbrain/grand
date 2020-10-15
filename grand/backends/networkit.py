from typing import Hashable, Generator, Iterable
import abc

import networkit
import pandas as pd

from .backend import Backend
from .metadatastore import MetadataStore, DictMetadataStore


class NodeNameManager:
    def __init__(self):
        self.node_names_by_id = {}
        self.node_ids_by_name = {}

    def add_node(self, name: Hashable, _id: Hashable):
        self.node_names_by_id[_id] = name
        self.node_ids_by_name[name] = _id

    def get_name(self, _id: Hashable) -> Hashable:
        return self.node_names_by_id[_id]

    def get_id(self, name: Hashable) -> Hashable:
        return self.node_ids_by_name[name]

    def __contains__(self, name: Hashable) -> bool:
        return name in self.node_ids_by_name


class NetworkitBackend(Backend):
    """
    Networkit doesn't support metadata or named nodes, so all node names and
    metadata must currently be stored in a parallel data structure.

    Networkit.graph.Graph documentation:
    https://networkit.github.io/dev-docs/python_api/graph.html

    """

    def __init__(self, directed: bool = False, metadata_store: MetadataStore = None):
        """
        Create a new Backend instance.

        Arguments:
            directed (bool: False): Whether to make the backend graph directed

        Returns:
            None

        """
        self._directed = directed
        self._meta = metadata_store or DictMetadataStore()
        self._nk_graph = networkit.graph.Graph()
        self._names = NodeNameManager()

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
        # TODO: Remove metadata from lookup if insertion fails
        nk_id = self._nk_graph.addNode()
        self._names.add_node(node_name, nk_id)
        self._meta.add_node(node_name, metadata)
        return nk_id

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        return self._meta.get_node(node_name)

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
            return [
                (self._names.get_name(i), self._meta.get_node(self._names.get_name(i)))
                for i in self._nk_graph.iterNodes()
            ]
        return [self._names.get_name(i) for i in self._nk_graph.iterNodes()]

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        return u in self._names

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
        # If u doesn't exist:
        if self.has_node(u):
            x = self._names.get_id(u)
        else:
            x = self.add_node(u, None)

        if self.has_node(v):
            y = self._names.get_id(v)
        else:
            y = self.add_node(v, None)

        # Insert metadata for this edge, replacing the previous metadata:
        self._meta.add_edge(u, v, metadata)

        # TODO: Support multigraphs, and allow duplicate edges.
        if self.has_edge(u, v):
            return
        return self._nk_graph.addEdge(x, y)

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        if include_metadata:
            return [(u, v) for u, v in self._nk_graph.iterEdges()]
        return [(u, v) for u, v in self._nk_graph.iterEdges()]

    def has_edge(self, u, v):
        val = self._nk_graph.hasEdge(self._names.get_id(u), self._names.get_id(v))
        print(self._names.get_id(u), self._names.get_id(v), val)
        return val

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        if self.has_edge(u, v):
            return self._meta.get_edge(u, v)
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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def get_node_count(self) -> Iterable:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return len([i for i in self.all_nodes_as_iterable()])
