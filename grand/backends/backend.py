from typing import Hashable, Collection, Iterable
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
            upsert (bool: True): Update the node if it already exists. If this
                is set to False and the node already exists, a backend may
                choose to throw an error or proceed gracefully.

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

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Collection:
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

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Collection:
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
        ...

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
        ...

    def get_node_count(self) -> int:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return len([i for i in self.all_nodes_as_iterable()])

    def degree(self, u: Hashable) -> int:
        """
        Get the degree of a node.

        Arguments:
            u (Hashable): The node ID

        Returns:
            int: The degree of the node

        """
        return len(self.get_node_neighbors(u))

    def degrees(self, nbunch=None) -> Collection:
        return {
            node: self.degree(node) for node in (nbunch or self.all_nodes_as_iterable())
        }

    def in_degree(self, u: Hashable) -> int:
        """
        Get the in-degree of a node.

        Arguments:
            u (Hashable): The node ID

        Returns:
            int: The in-degree of the node

        """
        return len(list(self.get_node_predecessors(u)))

    def in_degrees(self, nbunch=None) -> Collection:
        nbunch = nbunch or self.all_nodes_as_iterable()
        if isinstance(nbunch, (list, tuple)):
            return {node: self.in_degree(node) for node in nbunch}
        else:
            return self.in_degree(nbunch)

    def out_degree(self, u: Hashable) -> int:
        """
        Get the out-degree of a node.

        Arguments:
            u (Hashable): The node ID

        Returns:
            int: The out-degree of the node

        """
        return len(list(self.get_node_successors(u)))

    def out_degrees(self, nbunch=None) -> Collection:
        nbunch = nbunch or self.all_nodes_as_iterable()
        if isinstance(nbunch, (list, tuple)):
            return {node: self.out_degree(node) for node in nbunch}
        else:
            return self.out_degree(nbunch)
