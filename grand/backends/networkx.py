from typing import Hashable, Generator, Iterable
import time

import boto3
import pandas as pd
import networkx as nx

from .backend import Backend


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
        self._directed = directed

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

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Generator:
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

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
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
            return self._nx_graph[u]
        return self._nx_graph.neighbors(u)

    def get_node_predecessors(
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
            return self._nx_graph.pred[u]
        return self._nx_graph.predecessors(u)

    def get_node_count(self) -> Iterable:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return len(self._nx_graph)

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> None:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """

        tic = time.time()
        self._nx_graph.add_edges_from(
            [
                (
                    e[source_column],
                    e[target_column],
                    {
                        k: v
                        for k, v in dict(e).items()
                        if k not in [source_column, target_column]
                    },
                )
                for _, e in edgelist.iterrows()
            ]
        )

        nodes = edgelist[source_column].append(edgelist[target_column]).unique()

        return {
            "node_count": len(nodes),
            "node_duration": 0,
            "edge_count": len(edgelist),
            "edge_duration": time.time() - tic,
        }

    def teardown(self) -> None:
        return
