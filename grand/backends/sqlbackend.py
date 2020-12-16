from typing import Hashable, Generator, Optional, Iterable
import time

import pandas as pd
import sqlalchemy
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select
from sqlalchemy import and_, or_, func

from .backend import Backend

_DEFAULT_SQL_URL = "sqlite:///"
_DEFAULT_SQL_STR_LEN = 64


class SQLBackend(Backend):
    """
    A graph datastore that uses a SQL-like store for persistance and queries.

    """

    def __init__(
        self,
        directed: bool = False,
        node_table_name: str = None,
        edge_table_name: str = None,
        db_url: str = _DEFAULT_SQL_URL,
        primary_key: str = "ID",
        sqlalchemy_kwargs: dict = None,
    ) -> None:
        """
        Create a new SQL-backed graph store.

        Arguments:
            node_table_name (str: "grand_Nodes"): The name to use for the node
                table in DynamoDB.
            edge_table_name (str: "grand_Edges"): The name to use for the edge
                table in DynamoDB.
            db_url (str: _DEFAULT_SQL_URL): The URL to use for the SQL db.
            primary_key (str: "ID"): The default primary key to use for the
                tables. Note that this key cannot exist in your metadata dicts.

        """
        self._directed = directed
        self._node_table_name = node_table_name or "grand_Nodes"
        self._edge_table_name = edge_table_name or "grand_Edges"

        self._primary_key = primary_key
        self._edge_source_key = "Source"
        self._edge_target_key = "Target"

        sqlalchemy_kwargs = sqlalchemy_kwargs or {}
        self._engine = sqlalchemy.create_engine(db_url, **sqlalchemy_kwargs)
        self._connection = self._engine.connect()
        self._metadata = sqlalchemy.MetaData()

        if not self._engine.dialect.has_table(self._connection, self._node_table_name):
            self._node_table = sqlalchemy.Table(
                self._node_table_name,
                self._metadata,
                sqlalchemy.Column(
                    self._primary_key,
                    sqlalchemy.String(_DEFAULT_SQL_STR_LEN),
                    primary_key=True,
                ),
                sqlalchemy.Column("_metadata", sqlalchemy.JSON),
            )
            self._node_table.create(self._engine)
        else:
            self._node_table = sqlalchemy.Table(
                self._node_table_name,
                self._metadata,
                autoload=True,
                autoload_with=self._engine,
            )

        if not self._engine.dialect.has_table(self._connection, self._edge_table_name):
            self._edge_table = sqlalchemy.Table(
                self._edge_table_name,
                self._metadata,
                sqlalchemy.Column(
                    self._primary_key,
                    sqlalchemy.String(_DEFAULT_SQL_STR_LEN),
                    primary_key=True,
                ),
                sqlalchemy.Column("_metadata", sqlalchemy.JSON),
                sqlalchemy.Column(
                    self._edge_source_key, sqlalchemy.String(_DEFAULT_SQL_STR_LEN)
                ),
                sqlalchemy.Column(
                    self._edge_target_key, sqlalchemy.String(_DEFAULT_SQL_STR_LEN)
                ),
            )
            self._edge_table.create(self._engine)
        else:
            self._edge_table = sqlalchemy.Table(
                self._edge_table_name,
                self._metadata,
                autoload=True,
                autoload_with=self._engine,
            )

    # def __del__(self):
    # self._connection.close()

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
        if yes_i_am_sure:
            self._node_table.drop(self._engine)
            self._edge_table.drop(self._engine)

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
        self._connection.execute(
            self._node_table.insert(),
            **{self._primary_key: node_name, "_metadata": metadata},
        )
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
        results = self._connection.execute(self._node_table.select()).fetchall()
        if include_metadata:
            return [(row[self._primary_key], row["_metadata"]) for row in results]
        return [row[self._primary_key] for row in results]

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        return len(
            self._connection.execute(
                self._node_table.select().where(
                    self._node_table.c[self._primary_key] == u
                )
            ).fetchall()
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
        pk = f"__{u}__{v}"

        if not self.has_node(u):
            self.add_node(u, {})
        if not self.has_node(v):
            self.add_node(v, {})

        try:
            self._connection.execute(
                self._edge_table.insert(),
                **{
                    self._primary_key: pk,
                    self._edge_source_key: u,
                    self._edge_target_key: v,
                    "_metadata": metadata,
                },
            )
        except sqlalchemy.exc.IntegrityError:
            # Edge already exists
            pass
        return pk

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        return iter(
            [
                (e.Source, e.Target, e._metadata)
                if include_metadata
                else (e.Source, e.Target)
                for e in self._connection.execute(self._edge_table.select()).fetchall()
            ]
        )

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        res = (
            self._connection.execute(
                self._node_table.select().where(
                    self._node_table.c[self._primary_key] == node_name
                )
            )
            .fetchone()
            ._metadata
        )
        return res

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
            pk = f"__{u}__{v}"
            return (
                self._connection.execute(
                    self._edge_table.select().where(
                        self._edge_table.c[self._primary_key] == pk
                    )
                )
                .fetchone()
                ._metadata
            )
        else:
            return (
                self._connection.execute(
                    self._edge_table.select().where(
                        or_(
                            (self._edge_table.c[self._primary_key] == f"__{u}__{v}"),
                            (self._edge_table.c[self._primary_key] == f"__{v}__{u}"),
                        )
                    )
                )
                .fetchone()
                ._metadata
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
        if self._directed:
            res = self._connection.execute(
                self._edge_table.select().where(
                    self._edge_table.c[self._edge_source_key] == u
                )
            ).fetchall()
        else:
            res = self._connection.execute(
                self._edge_table.select().where(
                    or_(
                        (self._edge_table.c[self._edge_source_key] == u),
                        (self._edge_table.c[self._edge_target_key] == u),
                    )
                )
            ).fetchall()

        if include_metadata:
            return {
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != u
                    else r[self._edge_target_key]
                ): r["_metadata"]
                for r in res
            }

        return iter(
            [
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != u
                    else r[self._edge_target_key]
                )
                for r in res
            ]
        )

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
        if self._directed:
            res = self._connection.execute(
                self._edge_table.select().where(
                    self._edge_table.c[self._edge_target_key] == u
                )
            ).fetchall()
        else:
            res = self._connection.execute(
                self._edge_table.select().where(
                    or_(
                        (self._edge_table.c[self._edge_target_key] == u),
                        (self._edge_table.c[self._edge_source_key] == u),
                    )
                )
            ).fetchall()

        if include_metadata:
            return {
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != u
                    else r[self._edge_target_key]
                ): r["_metadata"]
                for r in res
            }

        return iter(
            [
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != u
                    else r[self._edge_target_key]
                )
                for r in res
            ]
        )

    def get_node_count(self) -> Iterable:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return self._connection.execute(
            select([func.count()]).select_from(self._node_table)
        ).scalar()

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> None:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        # Produce edge list:

        edge_tic = time.time()
        newlist = edgelist.rename(
            columns={
                source_column: self._edge_source_key,
                target_column: self._edge_target_key,
            }
        )

        newlist[self._primary_key] = edgelist.apply(
            lambda x: f"__{x[source_column]}__{x[target_column]}", axis="columns"
        )
        newlist["_metadata"] = edgelist.apply(
            lambda x: {
                k: v for k, v in x.items() if k not in [source_column, target_column]
            },
            axis="columns",
        )

        newlist[
            [
                self._edge_source_key,
                self._edge_target_key,
                self._primary_key,
                "_metadata",
            ]
        ].to_sql(
            self._edge_table_name,
            self._engine,
            index=False,
            if_exists="append",
            dtype={"_metadata": sqlalchemy.JSON},
        )

        edge_toc = time.time() - edge_tic

        # now ingest nodes:
        node_tic = time.time()
        nodes = edgelist[source_column].append(edgelist[target_column]).unique()
        pd.DataFrame(
            [
                {
                    self._primary_key: node,
                    # no metadata:
                    "_metadata": {},
                }
                for node in nodes
            ]
        ).to_sql(
            self._node_table_name,
            self._engine,
            index=False,
            if_exists="replace",
            dtype={"_metadata": sqlalchemy.JSON},
        )

        return {
            "node_count": len(nodes),
            "node_duration": time.time() - node_tic,
            "edge_count": len(edgelist),
            "edge_duration": edge_toc,
        }

