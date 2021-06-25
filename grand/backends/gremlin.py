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
    """
    A backend instance for Gremlin-compatible graph databases.

    """

    def __init__(self, graph: GraphTraversalSource, directed: bool = True):
        """
        Create a new Backend instance wrapping a Gremlin endpoint.

        Arguments:
            directed (bool: False): Whether to make the backend graph directed

        Returns:
            None

        """
        self._g = graph

    def is_directed(self) -> bool:
        """
        Return True if the backend graph is directed.

        The Gremlin-backed datastore is always directed.

        Arguments:
            None

        Returns:
            bool: True if the backend graph is directed.

        """
        return True

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
            # Retrieve the existing node; we will update the props.
            v = self._g.V().has(ID, node_name)
        else:
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
            return iter(
                [
                    {n[ID][0]: _node_to_metadata(n)}
                    for n in self._g.V().valueMap(True).toList()
                ]
            )
        else:
            return iter([n[ID] for n in self._g.V().project(ID).by(ID).toList()])

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
        try:
            self.get_edge_by_id(u, v)
            e = self._g.V().has(ID, u).outE().as_("e").inV().has(ID, v).select("e")
        except IndexError:
            if not self.has_node(u):
                self.add_node(u, {})
            if not self.has_node(v):
                self.add_node(v, {})
            e = (
                self._g.V()
                .has(ID, u)
                .addE(EDGE_NAME)
                .as_("e")
                .to(__.V().has(ID, v))
                .select("e")
            )
        for key, val in metadata.items():
            e = e.property(key, val)
        return e.toList()

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        if include_metadata:
            return iter(
                [
                    (e["source"], e["target"], _node_to_metadata(e["properties"]))
                    for e in (
                        self._g.V()
                        .outE()
                        .project("target", "source", "properties")
                        .by(__.inV().values(ID))
                        .by(__.outV().values(ID))
                        .by(__.valueMap(True))
                        .toList()
                    )
                ]
            )
        return iter(
            [
                (e["source"], e["target"])
                for e in self._g.V()
                .outE()
                .project("target", "source")
                .by(__.inV().values(ID))
                .by(__.outV().values(ID))
                .toList()
            ]
        )

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
        )[0]

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
                e["target"]: _node_to_metadata(e["properties"])
                for e in (
                    self._g.V()
                    .has(ID, u)
                    .outE()
                    .project("target", "source", "properties")
                    .by(__.inV().values(ID))
                    .by(__.outV().values(ID))
                    .by(__.valueMap(True))
                    .toList()
                )
            }
        return self._g.V().has(ID, u).out().values(ID).toList()

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
            return {
                e["source"]: e
                for e in (
                    self._g.V()
                    .has(ID, u)
                    .inE()
                    .project("target", "source", "properties")
                    .by(__.inV().values(ID))
                    .by(__.outV().values(ID))
                    .by(__.valueMap(True))
                    .toList()
                )
            }
        return self._g.V().out().has(ID, u).values(ID).toList()

    def get_node_count(self) -> Iterable:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return self._g.V().count().toList()[0]

    def teardown(self) -> None:
        self._g.V().drop().toList()
