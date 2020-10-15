from typing import Hashable, Iterable
import abc

import pandas as pd


class Backend(abc.ABC):
    """
    Abstract base class for the management of persisted graph structure.

    Do not use this class directly.

    """

    def __init__(self, directed: bool = False):
        """
        Create a new Backend instance.

        Arguments:
            directed (bool: False): Whether to make the backend graph directed

        Returns:
            None

        """
        ...

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> None:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        ...

    def is_directed(self) -> bool:
        """
        Return True if the backend graph is directed.

        Arguments:
            None

        Returns:
            bool: True if the backend graph is directed.

        """
        ...

    def add_node(self, node_name: Hashable, metadata: dict):
        """
        Add a new node to the graph.

        Arguments:
            node_name (Hashable): The ID of the node
            metadata (dict: None): An optional dictionary of metadata

        Returns:
            Hashable: The ID of this node, as inserted

        """
        ...

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        ...

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Iterable:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        ...

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        ...

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
        ...

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Iterable:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        ...

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        ...

    def get_node_successors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Iterable:
        return self.get_node_neighbors(u, include_metadata)

    def get_node_neighbors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Iterable:
        """
        Get a generator of all downstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """
        ...

    def get_node_predecessors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Iterable:
        """
        Get a generator of all upstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """
        ...

    def get_node_count(self) -> Iterable:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return len([i for i in self.all_nodes_as_iterable()])
