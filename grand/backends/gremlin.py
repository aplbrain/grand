"""
https://tinkerpop.apache.org/docs/current/reference/
"""

from typing import Hashable, Generator, Iterable
import time

import pandas as pd
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __, GraphTraversalSource
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from .backend import Backend

ID = "__id"
EDGE_NAME = "__edge"
NODE_NAME = "__node"


def _node_to_metadata(n):
    return {k if isinstance(k, str) else k.name: v for k, v in n.items()}


class GremlinBackend(Backend):
    def __init__(self, graph: GraphTraversalSource, directed: bool = False):
        """
        Create a new Backend instance wrapping a Gremlin endpoint.

        Arguments:
            directed (bool: False): Whether to make the backend graph directed

        Returns:
            None

        """
        self._directed = directed
        self._g = graph

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
        if self.has_node(node_name):
            raise NotImplementedError("TODO: Update nodes")
        v = self._g.addV().property(ID, node_name)
        for key, val in metadata.items():
            v = v.property(key, val)
        return v.toList()[0]

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        try:
            return _node_to_metadata(
                self._g.V().has(ID, node_name).valueMap(True).toList()[0]
            )
        except IndexError as e:
            raise KeyError() from e

    def has_node(self, u: Hashable) -> bool:
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        try:
            self.get_node_by_id(u)
            return True
        except KeyError:
            return False

    def remove_node(self, node_name: Hashable):
        """
        Remove a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        return self._g.V().has(ID, node_name).drop().toList()

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
            return [_node_to_metadata(n) for n in self._g.V().valueMap(True).toList()]
        else:
            return [n["__id"] for n in self._g.V().project("__id").by("__id").toList()]

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
        if not self.has_node(u):
            self.add_node(u, {})
        if not self.has_node(v):
            self.add_node(v, {})
        return self._g.V().has(ID, u).addE(EDGE_NAME).to(__.V().has(ID, v)).toList()[0]

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        if include_metadata:
            raise NotImplementedError
        return [
            (e["source"], e["target"])
            for e in self._g.V()
            .outE()
            .project("target", "source")
            .by(__.inV().values(ID))
            .by(__.outV().values(ID))
            .toList()
        ]

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        return (
            self._g.V()
            .has(ID, u)
            .outE()
            .as_("e")
            .inV()
            .has(ID, v)
            .select("e")
            .properties()
            .toList()
        )

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
            raise NotImplementedError()
        return self._g.V().has(ID, u).out().toList()

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
            raise NotImplementedError()
        return self._g.V().out().has(ID, u).toList()

    def get_node_count(self) -> Iterable:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return self._g.V().count().toList()[0]

    # def ingest_from_edgelist_dataframe(
    #     self, edgelist: pd.DataFrame, source_column: str, target_column: str
    # ) -> None:
    #     """
    #     Ingest an edgelist from a Pandas DataFrame.

    #     """

    #     tic = time.time()
    #     self._nx_graph.add_edges_from(
    #         [
    #             (
    #                 e[source_column],
    #                 e[target_column],
    #                 {
    #                     k: v
    #                     for k, v in dict(e).items()
    #                     if k not in [source_column, target_column]
    #                 },
    #             )
    #             for _, e in edgelist.iterrows()
    #         ]
    #     )

    #     nodes = edgelist[source_column].append(edgelist[target_column]).unique()

    #     return {
    #         "node_count": len(nodes),
    #         "node_duration": 0,
    #         "edge_count": len(edgelist),
    #         "edge_duration": time.time() - tic,
    #     }

    def teardown(self) -> None:
        return
