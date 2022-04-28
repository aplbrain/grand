from typing import Hashable, Generator, Optional, Iterable
from xml.etree.ElementInclude import include

# import time

import pandas as pd

# import sqlalchemy
# from sqlalchemy.pool import NullPool
# from sqlalchemy.sql import select
# from sqlalchemy import and_, or_, func

from .backend import Backend

# _DEFAULT_SQL_URL = "sqlite:///"
# _DEFAULT_SQL_STR_LEN = 64


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
        if self._node_df is not None:
            if self.has_node(node_name):
                existing_metadata = self.get_node_by_id(node_name)
                existing_metadata.update(metadata)
                for k, v in existing_metadata.items():
                    self._node_df.loc[node_name, k] = v
            else:
                for k, v in metadata.items():
                    self._node_df.loc[node_name, k] = v
        else:
            self._node_df = pd.DataFrame(
                columns=[
                    self._node_df_id_column,
                    *metadata.keys(),
                ]
            )
            self._node_df.loc[0, self._node_df_id_column] = node_name
            for k, v in metadata.items():
                self._node_df.loc[0, k] = v
            self._node_df.set_index(self._node_df_id_column, inplace=True)
        return node_name

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        if self._node_df is not None:
            for node_id, row in self._node_df.iterrows():
                if include_metadata:
                    yield (
                        node_id,
                        dict(row),
                    )
                else:
                    yield node_id

        else:
            for u in self._edge_df[self._edge_df_source_column]:
                if include_metadata:
                    yield (u, dict())
                else:
                    yield u

            for u in self._edge_df[self._edge_df_target_column]:
                if include_metadata:
                    yield (u, dict())
                else:
                    yield u

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

    #     def add_edge(self, u: Hashable, v: Hashable, metadata: dict):
    #         """
    #         Add a new edge to the graph between two nodes.

    #         If the graph is directed, this edge will start (source) at the `u` node
    #         and end (target) at the `v` node.

    #         Arguments:
    #             u (Hashable): The source node ID
    #             v (Hashable): The target node ID
    #             metadata (dict): Optional metadata to associate with the edge

    #         Returns:
    #             Hashable: The edge ID, as inserted.

    #         """
    #         pk = f"__{u}__{v}"

    #         if not self.has_node(u):
    #             self.add_node(u, {})
    #         if not self.has_node(v):
    #             self.add_node(v, {})

    #         try:
    #             self._connection.execute(
    #                 self._edge_table.insert(),
    #                 **{
    #                     self._primary_key: pk,
    #                     self._edge_source_key: u,
    #                     self._edge_target_key: v,
    #                     "_metadata": metadata,
    #                 },
    #             )
    #         except sqlalchemy.exc.IntegrityError:
    #             # Edge already exists, perform the update:
    #             existing_metadata = self.get_edge_by_id(u, v)
    #             existing_metadata.update(metadata)
    #             self._connection.execute(
    #                 self._edge_table.update().where(
    #                     self._edge_table.c[self._primary_key] == pk
    #                 ),
    #                 **{"_metadata": existing_metadata},
    #             )

    #         return pk

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
            return dict(self._node_df.loc[node_name])

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
                ._metadata
            )

        else:
            left = self._edge_df[
                (self._edge_df[self._edge_df_source_column] == u)
                & (self._edge_df[self._edge_df_target_column] == v)
            ]
            if left:
                return left.iloc[0]._metadata
            right = self._edge_df[
                (self._edge_df[self._edge_df_source_column] == v)
                & (self._edge_df[self._edge_df_target_column] == u)
            ]
            if right:
                return right.iloc[0]._metadata

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
        if self._directed:
            raise NotImplementedError("Directed graphs not implemented")

        for _, row in self._edge_df[
            (self._edge_df[self._edge_df_source_column] == u)
        ].iterrows():
            if include_metadata:
                yield (
                    row[self._edge_df_target_column],
                    dict(row),
                )
            else:
                yield row[self._edge_df_target_column]

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
        if not self._directed:
            raise NotImplementedError("Undirected graphs not implemented")

        for _, row in self._edge_df[
            (self._edge_df[self._edge_df_target_column] == u)
        ].iterrows():
            if include_metadata:
                yield (
                    row[self._edge_df_source_column],
                    dict(row),
                )
            else:
                yield row[self._edge_df_source_column]

        # if include_metadata:
        #     raise NotImplementedError("Undirected graphs not implemented")
        #     return {
        #         (
        #             r[self._edge_source_key]
        #             if r[self._edge_source_key] != u
        #             else r[self._edge_target_key]
        #         ): r["_metadata"]
        #         for r in res
        #     }
        # for _, row in self._edge_df[
        #     (self._edge_df[self._edge_df_target_column] == u)
        # ].iterrows():

        # if self._directed:
        #     res = self._connection.execute(
        #         self._edge_table.select().where(
        #             self._edge_table.c[self._edge_target_key] == u
        #         )
        #     ).fetchall()
        # else:
        #     res = self._connection.execute(
        #         self._edge_table.select().where(
        #             or_(
        #                 (self._edge_table.c[self._edge_target_key] == u),
        #                 (self._edge_table.c[self._edge_source_key] == u),
        #             )
        #         )
        #     ).fetchall()

        # if include_metadata:
        #     return {
        #         (
        #             r[self._edge_source_key]
        #             if r[self._edge_source_key] != u
        #             else r[self._edge_target_key]
        #         ): r["_metadata"]
        #         for r in res
        #     }

        # return iter(
        #     [
        #         (
        #             r[self._edge_source_key]
        #             if r[self._edge_source_key] != u
        #             else r[self._edge_target_key]
        #         )
        #         for r in res
        #     ]
        # )

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


#     def out_degrees(self, nbunch=None):
#         """
#         Return the in-degree of each node in the graph.

#         Arguments:
#             nbunch (Iterable): The nodes to get the in-degree of

#         Returns:
#             dict: A dictionary of node: in-degree pairs

#         """

#         if nbunch is None:
#             where_clause = None
#         elif isinstance(nbunch, (list, tuple)):
#             where_clause = self._edge_table.c[self._edge_source_key].in_(nbunch)
#         else:
#             # single node:
#             where_clause = self._edge_table.c[self._edge_source_key] == nbunch

#         if self._directed:
#             query = (
#                 select([self._edge_table.c[self._edge_source_key], func.count()])
#                 .select_from(self._edge_table)
#                 .group_by(self._edge_table.c[self._edge_source_key])
#             )
#         else:
#             query = (
#                 select([self._edge_table.c[self._edge_source_key], func.count()])
#                 .select_from(self._edge_table)
#                 .group_by(self._edge_table.c[self._edge_source_key])
#             )

#         if where_clause is not None:
#             query = query.where(where_clause)

#         results = {
#             r[self._edge_source_key]: r[1]
#             for r in self._connection.execute(query).fetchall()
#         }

#         if nbunch and not isinstance(nbunch, (list, tuple)):
#             return results.get(nbunch, 0)
#         return results

#     def in_degrees(self, nbunch=None):
#         """
#         Return the in-degree of each node in the graph.

#         Arguments:
#             nbunch (Iterable): The nodes to get the in-degree of

#         Returns:
#             dict: A dictionary of node: in-degree pairs

#         """

#         if nbunch is None:
#             where_clause = None
#         elif isinstance(nbunch, (list, tuple)):
#             where_clause = self._edge_table.c[self._edge_target_key].in_(nbunch)
#         else:
#             # single node:
#             where_clause = self._edge_table.c[self._edge_target_key] == nbunch

#         if self._directed:
#             query = (
#                 select([self._edge_table.c[self._edge_target_key], func.count()])
#                 .select_from(self._edge_table)
#                 .group_by(self._edge_table.c[self._edge_target_key])
#             )
#         else:
#             query = (
#                 select([self._edge_table.c[self._edge_target_key], func.count()])
#                 .select_from(self._edge_table)
#                 .group_by(self._edge_table.c[self._edge_target_key])
#             )

#         if where_clause is not None:
#             query = query.where(where_clause)

#         results = {
#             r[self._edge_target_key]: r[1]
#             for r in self._connection.execute(query).fetchall()
#         }

#         if nbunch and not isinstance(nbunch, (list, tuple)):
#             return results.get(nbunch, 0)
#         return results

#     def ingest_from_edgelist_dataframe(
#         self, edgelist: pd.DataFrame, source_column: str, target_column: str
#     ) -> None:
#         """
#         Ingest an edgelist from a Pandas DataFrame.

#         """
#         # Produce edge list:

#         edge_tic = time.time()
#         newlist = edgelist.rename(
#             columns={
#                 source_column: self._edge_source_key,
#                 target_column: self._edge_target_key,
#             }
#         )

#         newlist[self._primary_key] = edgelist.apply(
#             lambda x: f"__{x[source_column]}__{x[target_column]}", axis="columns"
#         )
#         newlist["_metadata"] = edgelist.apply(
#             lambda x: {
#                 k: v for k, v in x.items() if k not in [source_column, target_column]
#             },
#             axis="columns",
#         )

#         newlist[
#             [
#                 self._edge_source_key,
#                 self._edge_target_key,
#                 self._primary_key,
#                 "_metadata",
#             ]
#         ].to_sql(
#             self._edge_table_name,
#             self._engine,
#             index=False,
#             if_exists="append",
#             dtype={"_metadata": sqlalchemy.JSON},
#         )

#         edge_toc = time.time() - edge_tic

#         # now ingest nodes:
#         node_tic = time.time()
#         nodes = edgelist[source_column].append(edgelist[target_column]).unique()
#         pd.DataFrame(
#             [
#                 {
#                     self._primary_key: node,
#                     # no metadata:
#                     "_metadata": {},
#                 }
#                 for node in nodes
#             ]
#         ).to_sql(
#             self._node_table_name,
#             self._engine,
#             index=False,
#             if_exists="replace",
#             dtype={"_metadata": sqlalchemy.JSON},
#         )

#         return {
#             "node_count": len(nodes),
#             "node_duration": time.time() - node_tic,
#             "edge_count": len(edgelist),
#             "edge_duration": edge_toc,
#         }
