from typing import Hashable, Generator
import time

import pandas as pd

from .backend import Backend


class DataFrameBackend(Backend):
    def __init__(
        self,
        directed: bool = False,
        edge_df: pd.DataFrame = None,
        node_df: pd.DataFrame = None,
        edge_df_source_column: str = "Source",
        edge_df_target_column: str = "Target",
        node_df_id_column: str = "id",
    ):
        """
        Create a new DataFrame backend.

        You must pass an edgelist. A nodelist is optional.

        Arguments:
            edge_df (pd.DataFrame): An edgelist dataframe with one edge per row
            directed (bool: False): Whether the graph is directed
            node_df (pd.DataFrame): A node metadata lookup
            edge_df_source_column (str): The name of the column in `edge_df` to
                use as the source of edges
            edge_df_target_column (str): The name of the column in `edge_df` to
                use as the target of edges
            node_df_id_column (str): The name of the column in `node_df` to
                use as the node ID
        """
        self._directed = directed
        self._edge_df = edge_df or pd.DataFrame(
            columns=[edge_df_source_column, edge_df_target_column]
        )
        self._node_df = node_df or None
        self._edge_df_source_column = edge_df_source_column
        self._edge_df_target_column = edge_df_target_column
        self._node_df_id_column = node_df_id_column

    def is_directed(self) -> bool:
        """
        Return True if the backend graph is directed.

        Arguments:
            None

        Returns:
            bool: True if the backend graph is directed.

        """
        return self._directed

    def teardown(self, yes_i_am_sure: bool = False):
        """
        Tear down this graph, deleting all evidence it once was here.

        """
        return

    def add_node(self, node_name: Hashable, metadata: dict) -> Hashable:
        """
        Add a new node to the graph.

        Insert a new document into the nodes table.

        Arguments:
            node_name (Hashable): The ID of the node
            metadata (dict: None): An optional dictionary of metadata

        Returns:
            Hashable: The ID of this node, as inserted

        """

        # Add a new row to the nodes table:
        if self._node_df is None:
            self._node_df = pd.DataFrame(
                [
                    {
                        self._node_df_id_column: node_name,
                        **metadata,
                    }
                ],
                columns=[
                    self._node_df_id_column,
                    *metadata.keys(),
                ],
            )
            self._node_df.set_index(self._node_df_id_column, inplace=True)
        else:
            if self.has_node(node_name):
                existing_metadata = self.get_node_by_id(node_name)
                existing_metadata.update(metadata)
                for k, v in existing_metadata.items():
                    self._node_df.at[node_name, k] = v
            else:
                # Insert a new row:
                self._node_df = pd.concat(
                    [self._node_df, pd.DataFrame([{node_name: metadata}]).T]
                )

        return node_name

    def all_nodes_as_iterable(self, include_metadata: bool = False):
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        if self._node_df is not None:
            return [
                (
                    node_id,
                    row.to_dict(),
                )
                if include_metadata
                else node_id
                for node_id, row in self._node_df.iterrows()
            ]

        else:
            return [
                (node_id, {}) if include_metadata else node_id
                for node_id in self._edge_df[self._edge_df_source_column]
            ] + [
                (node_id, {}) if include_metadata else node_id
                for node_id in self._edge_df[self._edge_df_target_column]
            ]

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        if self._node_df is not None:
            return u in self._node_df.index

        return u in (self._edge_df[self._edge_df_source_column]) or u in (
            self._edge_df[self._edge_df_target_column]
        )

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

        if self._has_edge(u, v):
            # Update the existing edge:
            for k, m in metadata.items():
                if self._directed:
                    self._edge_df.loc[
                        (self._edge_df[self._edge_df_source_column] == u)
                        & (self._edge_df[self._edge_df_target_column] == v),
                        k,
                    ] = m
                else:
                    # Check for the edge in both directions:
                    self._edge_df.loc[
                        (self._edge_df[self._edge_df_source_column] == u)
                        & (self._edge_df[self._edge_df_target_column] == v),
                        k,
                    ] = m
                    self._edge_df.loc[
                        (self._edge_df[self._edge_df_source_column] == v)
                        & (self._edge_df[self._edge_df_target_column] == u),
                        k,
                    ] = m
        else:
            row = {
                self._edge_df_source_column: u,
                self._edge_df_target_column: v,
                **metadata,
            }
            self._edge_df.loc[len(self._edge_df)] = None
            for k, m in row.items():
                self._edge_df.loc[len(self._edge_df) - 1, k] = m
        return (u, v)

    def _has_edge(self, u: Hashable, v: Hashable) -> bool:
        """
        Return true if the edge exists in the graph.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            bool: True if the edge exists
        """
        if self._directed:
            return (
                (self._edge_df[self._edge_df_source_column] == u)
                & (self._edge_df[self._edge_df_target_column] == v)
            ).any()
        else:
            return (
                (self._edge_df[self._edge_df_source_column] == u)
                & (self._edge_df[self._edge_df_target_column] == v)
            ).any() or (
                (self._edge_df[self._edge_df_source_column] == v)
                & (self._edge_df[self._edge_df_target_column] == u)
            ).any()

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        for _, row in self._edge_df.iterrows():
            if include_metadata:
                yield (
                    row[self._edge_df_source_column],
                    row[self._edge_df_target_column],
                    dict(row),
                )
            else:
                yield (
                    row[self._edge_df_source_column],
                    row[self._edge_df_target_column],
                )

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        if self._node_df is not None:
            res = (self._node_df.loc[node_name]).to_dict()
            return res.get(0, res)

        return {}

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        if self._directed:
            return (
                self._edge_df[
                    (self._edge_df[self._edge_df_source_column] == u)
                    & (self._edge_df[self._edge_df_target_column] == v)
                ]
                .iloc[0]
                .to_dict()
            )

        else:
            left = self._edge_df[
                (self._edge_df[self._edge_df_source_column] == u)
                & (self._edge_df[self._edge_df_target_column] == v)
            ]
            if len(left):
                return self._edge_as_dict(left.iloc[0])
            right = self._edge_df[
                (self._edge_df[self._edge_df_source_column] == v)
                & (self._edge_df[self._edge_df_target_column] == u)
            ]
            if len(right):
                return self._edge_as_dict(right.iloc[0])

    def get_node_neighbors(self, u: Hashable, include_metadata: bool = False):
        """
        Get a generator of all downstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """

        if include_metadata:
            if self._directed:
                return {
                    (r[self._edge_df_target_column]): self._edge_as_dict(r)
                    for _, r in self._edge_df[
                        (self._edge_df[self._edge_df_source_column] == u)
                    ].iterrows()
                }
            else:
                return {
                    (
                        r[self._edge_df_source_column]
                        if r[self._edge_df_source_column] != u
                        else r[self._edge_df_target_column]
                    ): self._edge_as_dict(r)
                    for _, r in self._edge_df[
                        (self._edge_df[self._edge_df_source_column] == u)
                        | (self._edge_df[self._edge_df_target_column] == u)
                    ].iterrows()
                }

        if self._directed:
            return iter(
                [
                    row[self._edge_df_target_column]
                    for _, row in self._edge_df[
                        (self._edge_df[self._edge_df_source_column] == u)
                    ].iterrows()
                ]
            )
        else:
            return iter(
                [
                    row[self._edge_df_source_column]
                    if row[self._edge_df_source_column] != u
                    else row[self._edge_df_target_column]
                    for _, row in self._edge_df[
                        (self._edge_df[self._edge_df_source_column] == u)
                        | (self._edge_df[self._edge_df_target_column] == u)
                    ].iterrows()
                ]
            )

    def _edge_as_dict(self, row):
        """
        Convert an edge row to a dictionary.

        Arguments:
            row (pandas.Series): The edge row

        Returns:
            dict: The edge metadata

        """
        r = row.to_dict()
        r.pop(self._edge_df_source_column)
        r.pop(self._edge_df_target_column)
        return r

    def get_node_predecessors(self, u: Hashable, include_metadata: bool = False):
        """
        Get a generator of all upstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """

        if include_metadata:
            if self._directed:
                return {
                    (
                        r[self._edge_df_target_column]
                        if r[self._edge_df_target_column] != u
                        else r[self._edge_df_source_column]
                    ): self._edge_as_dict(r)
                    for _, r in self._edge_df[
                        (self._edge_df[self._edge_df_target_column] == u)
                    ].iterrows()
                }
            else:
                return {
                    (
                        r[self._edge_df_target_column]
                        if r[self._edge_df_target_column] != u
                        else r[self._edge_df_source_column]
                    ): self._edge_as_dict(r)
                    for _, r in self._edge_df[
                        (self._edge_df[self._edge_df_target_column] == u)
                        | (self._edge_df[self._edge_df_source_column] == u)
                    ].iterrows()
                }

        if self._directed:
            return iter(
                [
                    row[self._edge_df_source_column]
                    for _, row in self._edge_df[
                        (self._edge_df[self._edge_df_target_column] == u)
                    ].iterrows()
                ]
            )
        else:
            return iter(
                [
                    row[self._edge_df_source_column]
                    if row[self._edge_df_target_column] != u
                    else row[self._edge_df_target_column]
                    for _, row in self._edge_df[
                        (self._edge_df[self._edge_df_target_column] == u)
                        | (self._edge_df[self._edge_df_source_column] == u)
                    ].iterrows()
                ]
            )

    def get_node_count(self) -> int:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        if self._node_df is not None:
            return len(self._node_df)
        # Return number of unique sources intersected with number of unique targets
        return len(
            set(self._edge_df[self._edge_df_source_column]).intersection(
                set(self._edge_df[self._edge_df_target_column])
            )
        )

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> dict:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        # Produce edge list:

        edge_tic = time.time()
        self._edge_df = edgelist
        self._edge_df_source_column = source_column
        self._edge_df_target_column = target_column

        return {
            "edge_duration": time.time() - edge_tic,
        }
