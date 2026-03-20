from typing import Hashable, Generator
import time

import pandas as pd
import sqlalchemy
from sqlalchemy.sql import delete, select
from sqlalchemy import or_, func, Index

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
        edge_table_source_column: str = None,
        edge_table_target_column: str = None,
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
            edge_table_source_column (str: None): The name of the column to use
                for the source node in the edge table.
            edge_table_target_column (str: None): The name of the column to use
                for the target node in the edge table.

        """
        self._directed = directed
        self._node_table_name = node_table_name or "grand_Nodes"
        self._edge_table_name = edge_table_name or "grand_Edges"

        self._primary_key = primary_key
        self._edge_source_key = edge_table_source_column or "Source"
        self._edge_target_key = edge_table_target_column or "Target"

        sqlalchemy_kwargs = sqlalchemy_kwargs or {}
        self._engine = sqlalchemy.create_engine(db_url, **sqlalchemy_kwargs)
        self._connection = self._engine.connect()
        self._metadata = sqlalchemy.MetaData()

        # Create nodes table
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
        self._node_table.create(self._engine, checkfirst=True)

        source_column = sqlalchemy.Column(
            self._edge_source_key, sqlalchemy.String(_DEFAULT_SQL_STR_LEN)
        )

        target_column = sqlalchemy.Column(
            self._edge_target_key, sqlalchemy.String(_DEFAULT_SQL_STR_LEN)
        )

        # Create edges table
        self._edge_table = sqlalchemy.Table(
            self._edge_table_name,
            self._metadata,
            sqlalchemy.Column(
                self._primary_key,
                sqlalchemy.String(_DEFAULT_SQL_STR_LEN),
                primary_key=True,
            ),
            sqlalchemy.Column("_metadata", sqlalchemy.JSON),
            source_column,
            target_column,
        )
        self._edge_table.create(self._engine, checkfirst=True)

        # Create source and target index
        sindex = Index("edge_source", source_column)
        sindex.create(self._engine, checkfirst=True)

        tindex = Index("edge_target", target_column)
        tindex.create(self._engine, checkfirst=True)

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
        if self.has_node(node_name):
            existing_metadata = self.get_node_by_id(node_name)
            existing_metadata.update(metadata)
            self._connection.execute(
                self._node_table.update().where(
                    self._node_table.c[self._primary_key] == str(node_name)
                ),
                parameters={"_metadata": existing_metadata},
            )
        else:
            self._connection.execute(
                self._node_table.insert(),
                parameters={self._primary_key: node_name, "_metadata": metadata},
            )
        return node_name

    def _insert_empty_node_if_missing(self, node_name: Hashable) -> None:
        node_name = str(node_name)
        insert_statement = self._node_table.insert()

        # Use dialect-native "ignore duplicate key" behavior when available to
        # avoid the extra existence query on every edge insertion.
        if self._engine.dialect.name == "sqlite":
            self._connection.execute(
                insert_statement.prefix_with("OR IGNORE"),
                parameters={self._primary_key: node_name, "_metadata": {}},
            )
            return

        if self._engine.dialect.name in {"mysql", "mariadb"}:
            self._connection.execute(
                insert_statement.prefix_with("IGNORE"),
                parameters={self._primary_key: node_name, "_metadata": {}},
            )
            return

        if not self.has_node(node_name):
            self._connection.execute(
                insert_statement,
                parameters={self._primary_key: node_name, "_metadata": {}},
            )

    def add_nodes_from(self, nodes_for_adding, **attr):
        nodes = [
            {
                self._primary_key: node,
                "_metadata": {**attr, **metadata},
            }
            for node, metadata in nodes_for_adding
        ]

        self._connection.execute(self._node_table.insert(), nodes)

    def _upsert_node(self, node_name: Hashable, metadata: dict) -> Hashable:
        """
        Add a new node to the graph, or update an existing one.

        Arguments:
            node_name (Hashable): The ID of the node
            metadata (dict: None): An optional dictionary of metadata

        Returns:
            Hashable: The ID of this node, as inserted

        """
        node_exists = self.has_node(node_name)
        if node_exists:
            self._connection.execute(
                self._node_table.update().where(
                    self._node_table.c[self._primary_key] == str(node_name)
                ),
                parameters={"_metadata": metadata},
            )
        else:
            self._connection.execute(
                self._node_table.insert(),
                parameters={self._primary_key: node_name, "_metadata": metadata},
            )

    def remove_node(self, u: Hashable) -> None:
        """
        Removes nodes and related edges for name.

        Args:
            u (Hashable): id of the node
        """

        # Remove nodes
        statement = delete(self._node_table).where(
            self._node_table.c[self._primary_key] == str(u)
        )
        self._connection.execute(statement)

        # Remove edges for node
        statement = delete(self._edge_table).where(
            or_(
                self._edge_table.c[self._edge_source_key] == str(u),
                self._edge_table.c[self._edge_target_key] == str(u)
            )
        )
        self._connection.execute(statement)

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
            sql = self._node_table.select()
        else:
            sql = self._node_table.select().with_only_columns(
                self._node_table.c[self._primary_key]
            )

        results = []
        for x in self._connection.execute(sql):
            results.append(x if include_metadata else x[0])

        return results

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        return (
            self._connection.execute(
                select(self._node_table.c[self._primary_key]).where(
                    self._node_table.c[self._primary_key] == str(u)
                )
            ).fetchone()
            is not None
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

        self._insert_empty_node_if_missing(u)
        self._insert_empty_node_if_missing(v)

        try:
            self._connection.execute(
                self._edge_table.insert(),
                parameters={
                    self._primary_key: pk,
                    self._edge_source_key: u,
                    self._edge_target_key: v,
                    "_metadata": metadata,
                },
            )
        except sqlalchemy.exc.IntegrityError:
            # Edge already exists, perform the update:
            existing_metadata = self.get_edge_by_id(u, v)
            existing_metadata.update(metadata)
            self._connection.execute(
                self._edge_table.update().where(
                    self._edge_table.c[self._primary_key] == pk
                ),
                parameters={"_metadata": existing_metadata},
            )

        return pk

    def add_edges_from(self, ebunch_to_add, **attr):
        edges = [
            {
                self._primary_key: f"__{u}__{v}",
                self._edge_source_key: u,
                self._edge_target_key: v,
                "_metadata": {**attr, **metadata},
            }
            for u, v, metadata in ebunch_to_add
        ]

        self._connection.execute(self._edge_table.insert(), edges)

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)
        """

        columns = [
            self._edge_table.c[self._edge_source_key],
            self._edge_table.c[self._edge_target_key],
        ]

        if include_metadata:
            columns.append(self._edge_table.c["_metadata"])

        sql = self._edge_table.select().with_only_columns(*columns)
        return self._connection.execute(sql).fetchall()

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """

        res = self._connection.execute(
            self._node_table.select().where(
                self._node_table.c[self._primary_key] == str(node_name)
            )
        ).fetchone()

        if res:
            return res._metadata

        raise KeyError(f"Node {node_name} not found")

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
            result = self._connection.execute(
                self._edge_table.select().where(
                    self._edge_table.c[self._primary_key] == pk
                )
            ).fetchone()
            if result:
                return result._metadata
            raise KeyError(f"Edge {u}-{v} not found.")
        else:
            result = self._connection.execute(
                self._edge_table.select().where(
                    or_(
                        (self._edge_table.c[self._primary_key] == f"__{u}__{v}"),
                        (self._edge_table.c[self._primary_key] == f"__{v}__{u}"),
                    )
                )
            ).fetchone()
            if result:
                return result._metadata
            raise KeyError(f"Edge {u}-{v} not found.")

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
                self._edge_table.select()
                .where(self._edge_table.c[self._edge_source_key] == str(u))
                .order_by(self._edge_table.c[self._primary_key])
            ).fetchall()
        else:
            res = self._connection.execute(
                self._edge_table.select()
                .where(
                    or_(
                        (self._edge_table.c[self._edge_source_key] == str(u)),
                        (self._edge_table.c[self._edge_target_key] == str(u)),
                    )
                )
                .order_by(self._edge_table.c[self._primary_key])
            ).fetchall()

        res = [x._asdict() for x in res]

        if include_metadata:
            return {
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != str(u)
                    else r[self._edge_target_key]
                ): r["_metadata"]
                for r in res
            }

        return iter(
            [
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != str(u)
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
                self._edge_table.select()
                .where(self._edge_table.c[self._edge_target_key] == str(u))
                .order_by(self._edge_table.c[self._primary_key])
            ).fetchall()
        else:
            res = self._connection.execute(
                self._edge_table.select()
                .where(
                    or_(
                        (self._edge_table.c[self._edge_target_key] == str(u)),
                        (self._edge_table.c[self._edge_source_key] == str(u)),
                    )
                )
                .order_by(self._edge_table.c[self._primary_key])
            ).fetchall()

        res = [x._asdict() for x in res]

        if include_metadata:
            return {
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != str(u)
                    else r[self._edge_target_key]
                ): r["_metadata"]
                for r in res
            }

        return iter(
            [
                (
                    r[self._edge_source_key]
                    if r[self._edge_source_key] != str(u)
                    else r[self._edge_target_key]
                )
                for r in res
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
        return self._connection.execute(
            select(func.count()).select_from(self._node_table)
        ).scalar()

    def get_edge_count(self) -> int:
        """
        Get an integer count of the number of edges in this graph.

        Arguments:
            None

        Returns:
            int: The count of edges

        """
        return self._connection.execute(
            select(func.count()).select_from(self._edge_table)
        ).scalar()

    def out_degrees(self, nbunch=None):
        """
        Return the in-degree of each node in the graph.

        Arguments:
            nbunch (Iterable): The nodes to get the in-degree of

        Returns:
            dict: A dictionary of node: in-degree pairs

        """

        if nbunch is None:
            where_clause = None
        elif isinstance(nbunch, (list, tuple)):
            where_clause = self._edge_table.c[self._edge_source_key].in_(
                [str(x) for x in nbunch]
            )
        else:
            # single node:
            where_clause = self._edge_table.c[self._edge_source_key] == str(nbunch)

        if self._directed:
            query = (
                select(self._edge_table.c[self._edge_source_key], func.count())
                .select_from(self._edge_table)
                .group_by(self._edge_table.c[self._edge_source_key])
            )
        else:
            query = (
                select(self._edge_table.c[self._edge_source_key], func.count())
                .select_from(self._edge_table)
                .group_by(self._edge_table.c[self._edge_source_key])
            )

        if where_clause is not None:
            query = query.where(where_clause)

        results = {r[0]: r[1] for r in self._connection.execute(query)}

        if nbunch and not isinstance(nbunch, (list, tuple)):
            return results.get(nbunch, 0)
        return results

    def in_degrees(self, nbunch=None):
        """
        Return the in-degree of each node in the graph.

        Arguments:
            nbunch (Iterable): The nodes to get the in-degree of

        Returns:
            dict: A dictionary of node: in-degree pairs

        """

        if nbunch is None:
            where_clause = None
        elif isinstance(nbunch, (list, tuple)):
            where_clause = self._edge_table.c[self._edge_target_key].in_(
                [str(x) for x in nbunch]
            )
        else:
            # single node:
            where_clause = self._edge_table.c[self._edge_target_key] == str(nbunch)

        if self._directed:
            query = (
                select(self._edge_table.c[self._edge_target_key], func.count())
                .select_from(self._edge_table)
                .group_by(self._edge_table.c[self._edge_target_key])
            )
        else:
            query = (
                select(self._edge_table.c[self._edge_target_key], func.count())
                .select_from(self._edge_table)
                .group_by(self._edge_table.c[self._edge_target_key])
            )

        if where_clause is not None:
            query = query.where(where_clause)

        results = {r[0]: r[1] for r in self._connection.execute(query)}

        if nbunch and not isinstance(nbunch, (list, tuple)):
            return results.get(nbunch, 0)
        return results

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> dict:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        # Produce edge list:

        edge_tic = time.time()
        edge_columns = [
            c for c in edgelist.columns if c not in [source_column, target_column]
        ]
        sources = [str(value) for value in edgelist[source_column].tolist()]
        targets = [str(value) for value in edgelist[target_column].tolist()]
        edge_metadata = (
            edgelist[edge_columns].to_dict("records")
            if edge_columns
            else [{} for _ in range(len(edgelist))]
        )

        edge_rows = [
            {
                self._edge_source_key: source,
                self._edge_target_key: target,
                self._primary_key: f"__{source}__{target}",
                "_metadata": metadata,
            }
            for source, target, metadata in zip(sources, targets, edge_metadata)
        ]

        if edge_rows:
            self._connection.execute(self._edge_table.insert(), edge_rows)

        edge_toc = time.time() - edge_tic

        # now ingest nodes:
        node_tic = time.time()
        nodes = pd.unique(
            pd.concat(
                [edgelist[source_column], edgelist[target_column]],
                ignore_index=True,
            )
        )

        node_rows = [
            {
                self._primary_key: str(node),
                "_metadata": {},
            }
            for node in nodes
        ]

        if node_rows:
            node_insert = self._node_table.insert()
            if self._engine.dialect.name == "sqlite":
                node_insert = node_insert.prefix_with("OR IGNORE")
                self._connection.execute(node_insert, node_rows)
            elif self._engine.dialect.name in {"mysql", "mariadb"}:
                node_insert = node_insert.prefix_with("IGNORE")
                self._connection.execute(node_insert, node_rows)
            else:
                for node in nodes:
                    self._insert_empty_node_if_missing(node)

        return {
            "node_count": len(nodes),
            "node_duration": time.time() - node_tic,
            "edge_count": len(edgelist),
            "edge_duration": edge_toc,
        }

    def commit(self):
        self._connection.commit()

    def close(self):
        self._connection.close()
