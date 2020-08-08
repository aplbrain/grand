"""
Grand graph databasifier.

Aug 2020
"""

import abc
from typing import Hashable, Generator

import networkx as nx


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

    def all_nodes_as_generator(self, include_metadata: bool = False) -> Generator:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

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

    def all_edges_as_generator(self, include_metadata: bool = False) -> Generator:
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


class NetworkXBackend(Backend):
    def __init__(self, directed: bool = False):
        """
        Create a new Backend instance.

        Arguments:
            directed (bool: False): Whether to make the backend graph directed

        Returns:
            None

        """
        self._nx_graph = nx.DiGraph() if directed else nx.Graph()

    def add_node(self, node_name: Hashable, metadata: dict):
        """
        Add a new node to the graph.

        Arguments:
            node_name (Hashable): The ID of the node
            metadata (dict: None): An optional dictionary of metadata

        Returns:
            Hashable: The ID of this node, as inserted

        """
        self._nx_graph.add_node(node_name, **metadata)

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        return self._nx_graph.nodes[node_name]

    def all_nodes_as_generator(self, include_metadata: bool = False) -> Generator:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        return self._nx_graph.nodes(data=include_metadata)

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
        self._nx_graph.add_edge(u, v, **metadata)

    def all_edges_as_generator(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        return self._nx_graph.edges(data=include_metadata)

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
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
