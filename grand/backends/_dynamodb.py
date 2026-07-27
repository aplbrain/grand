from typing import Collection, Hashable, Optional
import time

import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key

from .backend import Backend
from ._edge_identity import edge_identity


_DEFAULT_DYNAMODB_URL = "http://localhost:4566"
_EDGE_SOURCE_INDEX = "grand_Source"
_EDGE_TARGET_INDEX = "grand_Target"


def _dynamo_table_exists(table_name: str, client: boto3.client):
    """
    Check to see if the DynamoDB table already exists.

    Returns:
        bool: Whether table exists

    """
    existing_tables = client.list_tables()["TableNames"]
    return table_name in existing_tables


def _create_dynamo_table(
    table_name: str,
    primary_key: str,
    client,
    read_write_units: Optional[int] = None,
    index_keys: tuple[str, ...] = (),
):
    if read_write_units is not None:
        raise NotImplementedError("Non-on-demand billing is not currently supported.")

    table_kwargs = {
        "TableName": table_name,
        "KeySchema": [
            {"AttributeName": primary_key, "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": primary_key, "AttributeType": "S"},
            *[
                {"AttributeName": index_key, "AttributeType": "S"}
                for index_key in index_keys
            ],
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    if index_keys:
        table_kwargs["GlobalSecondaryIndexes"] = [
            {
                "IndexName": index_name,
                "KeySchema": [{"AttributeName": index_key, "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
            for index_name, index_key in zip(
                (_EDGE_SOURCE_INDEX, _EDGE_TARGET_INDEX), index_keys
            )
        ]

    return client.create_table(
        **table_kwargs,
    )


class DynamoDBBackend(Backend):
    """
    A graph datastore that uses DynamoDB for persistance and queries.

    """

    def __init__(
        self,
        directed: bool = False,
        node_table_name: str = None,
        edge_table_name: str = None,
        dynamodb_url: str = _DEFAULT_DYNAMODB_URL,
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
        primary_key: str = "ID",
    ) -> None:
        """
        Create a new dynamodb-backed graph store.

        Arguments:
            node_table_name (str: "grand_Nodes"): The name to use for the node
                table in DynamoDB.
            edge_table_name (str: "grand_Edges"): The name to use for the edge
                table in DynamoDB.
            dynamodb_url (str: _DEFAULT_DYNAMODB_URL): The URL to use for the
                DynamoDB resource. Defaults to AWS us-east-1.
            primary_key (str: "ID"): The default primary key to use for the
                tables. Note that this key cannot exist in your metadata dicts.

        """
        self._directed = directed
        self._node_table_name = node_table_name or "grand_Nodes"
        self._edge_table_name = edge_table_name or "grand_Edges"

        self._primary_key = primary_key
        self._edge_source_key = "Source"
        self._edge_target_key = "Target"

        self._resource = boto3.resource(
            "dynamodb",
            endpoint_url=dynamodb_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self._client = boto3.client(
            "dynamodb",
            endpoint_url=dynamodb_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        if not _dynamo_table_exists(self._node_table_name, self._client):
            node_creation_response = _create_dynamo_table(
                self._node_table_name, self._primary_key, self._resource
            )
            # Await table creation:
            if node_creation_response:
                node_creation_response.meta.client.get_waiter("table_exists").wait(
                    TableName=self._node_table_name
                )

        if not _dynamo_table_exists(self._edge_table_name, self._client):
            edge_creation_response = _create_dynamo_table(
                self._edge_table_name,
                self._primary_key,
                self._resource,
                index_keys=(self._edge_source_key, self._edge_target_key),
            )
            # Await table creation:
            if edge_creation_response:
                edge_creation_response.meta.client.get_waiter("table_exists").wait(
                    TableName=self._edge_table_name
                )

        self._node_table = self._resource.Table(self._node_table_name)
        self._edge_table = self._resource.Table(self._edge_table_name)

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
            self._node_table.delete()
            self._edge_table.delete()

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
        metadata[self._primary_key] = str(node_name)
        response = self._node_table.put_item(Item=metadata)

        return response

    def _scan_table(self, table, scan_kwargs: dict = None):
        done = False
        start_key = None
        results = []
        scan_kwargs = scan_kwargs or {}
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = table.scan(**scan_kwargs)
            results += response.get("Items", [])
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        return results

    def _query_edges(self, index_name: str, key: str, value: Hashable):
        results = []
        query_kwargs = {
            "IndexName": index_name,
            "KeyConditionExpression": Key(key).eq(str(value)),
        }
        while True:
            response = self._edge_table.query(**query_kwargs)
            results.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey")
            if start_key is None:
                return results
            query_kwargs["ExclusiveStartKey"] = start_key

    def _incident_edges(self, u: Hashable):
        outgoing = self._query_edges(_EDGE_SOURCE_INDEX, self._edge_source_key, u)
        incoming = self._query_edges(_EDGE_TARGET_INDEX, self._edge_target_key, u)
        return {
            edge[self._primary_key]: edge
            for edge in [*outgoing, *incoming]
        }.values()

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Collection:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        return [
            (
                (
                    node[self._primary_key],
                    {k: v for k, v in node.items() if k not in [self._primary_key]},
                )
                if include_metadata
                else node[self._primary_key]
            )
            for node in self._scan_table(self._node_table)
        ]

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        response = self._node_table.get_item(
            Key={self._primary_key: str(u)},
        )
        return "Item" in response

    def _edge_item(self, u: Hashable, v: Hashable):
        item = self._edge_table.get_item(
            Key={self._primary_key: edge_identity(u, v)}
        ).get("Item")
        if item is not None:
            return item
        for item in self._query_edges(_EDGE_SOURCE_INDEX, self._edge_source_key, u):
            if item[self._edge_target_key] == str(v):
                return item
        return None

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
        if self._edge_source_key in metadata:
            raise KeyError(
                f"'{self._edge_source_key}' should not be in metadata. I need that for PK!"
            )
        if self._edge_target_key in metadata:
            raise KeyError(
                f"'{self._edge_target_key}' should not be in metadata. I need that for PK!"
            )
        existing = self._edge_item(u, v)
        item = {
            **({} if existing is None else existing),
            **metadata,
            self._primary_key: (
                edge_identity(u, v)
                if existing is None
                else existing[self._primary_key]
            ),
            self._edge_source_key: str(u),
            self._edge_target_key: str(v),
        }

        if not self.has_node(u):
            self._node_table.put_item(Item={self._primary_key: str(u)})
        if not self.has_node(v):
            self._node_table.put_item(Item={self._primary_key: str(v)})

        response = self._edge_table.put_item(Item=item)

        return response

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Collection:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        return [
            (
                (edge[self._edge_source_key], edge[self._edge_target_key], edge)
                if include_metadata
                else (edge[self._edge_source_key], edge[self._edge_target_key])
            )
            for edge in self._scan_table(self._edge_table)
        ]

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        response = self._node_table.get_item(Key={self._primary_key: node_name})

        item = response["Item"]
        item.pop(self._primary_key)
        return item

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        item = self._edge_item(u, v)
        if item is None and not self._directed:
            item = self._edge_item(v, u)
        if item is None:
            raise KeyError(f"Edge {u}-{v} not found.")
        item.pop(self._primary_key)
        item.pop(self._edge_source_key)
        item.pop(self._edge_target_key)
        return item

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
        u = str(u)
        if self._directed:
            res = self._query_edges(
                _EDGE_SOURCE_INDEX,
                self._edge_source_key,
                u,
            )

        else:
            res = self._incident_edges(u)

        if include_metadata:
            results = {}
            for item in res:
                key = (
                    item[self._edge_source_key]
                    if item[self._edge_source_key] != u
                    else item[self._edge_target_key]
                )
                item.pop(self._primary_key)
                item.pop(self._edge_source_key)
                item.pop(self._edge_target_key)
                results[key] = item
            return results
        return iter(
            [
                (
                    edge[self._edge_source_key]
                    if edge[self._edge_source_key] != u
                    else edge[self._edge_target_key]
                )
                for edge in res
            ]
        )

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
        u = str(u)
        if self._directed:
            res = self._query_edges(
                _EDGE_TARGET_INDEX,
                self._edge_target_key,
                u,
            )

        else:
            res = self._incident_edges(u)

        if include_metadata:
            results = {}
            for item in res:
                key = (
                    item[self._edge_source_key]
                    if item[self._edge_source_key] != u
                    else item[self._edge_target_key]
                )
                item.pop(self._primary_key)
                item.pop(self._edge_source_key)
                item.pop(self._edge_target_key)
                results[key] = item
            return results
        return iter(
            [
                (
                    edge[self._edge_source_key]
                    if edge[self._edge_source_key] != u
                    else edge[self._edge_target_key]
                )
                for edge in res
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
        return self._client.describe_table(TableName=self._node_table_name)["Table"][
            "ItemCount"
        ]

    def get_edge_count(self) -> int:
        """
        Get an integer count of the number of edges in this graph.

        Arguments:
            None

        Returns:
            int: The count of edges

        """
        return self._client.describe_table(TableName=self._edge_table_name)["Table"][
            "ItemCount"
        ]

    def degrees(self, nbunch=None) -> Collection:
        if nbunch is not None and not isinstance(nbunch, (list, tuple)):
            return self.degree(nbunch)

        requested = None if nbunch is None else list(nbunch)
        degrees = {} if requested is None else {node: 0 for node in requested}
        requested_by_id = (
            None if requested is None else {str(node): node for node in requested}
        )
        for edge in self._scan_table(self._edge_table):
            source = edge[self._edge_source_key]
            target = edge[self._edge_target_key]
            if requested_by_id is None:
                degrees[source] = degrees.get(source, 0) + 1
                if self._directed or target != source:
                    degrees[target] = degrees.get(target, 0) + 1
                continue
            if source in requested_by_id:
                node = requested_by_id[source]
                degrees[node] += 1
            if target in requested_by_id and (self._directed or target != source):
                node = requested_by_id[target]
                degrees[node] += 1
        return degrees

    def in_degrees(self, nbunch=None) -> Collection:
        if not self._directed:
            return self.degrees(nbunch)
        return self._directed_bulk_degrees(nbunch, self._edge_target_key)

    def out_degrees(self, nbunch=None) -> Collection:
        if not self._directed:
            return self.degrees(nbunch)
        return self._directed_bulk_degrees(nbunch, self._edge_source_key)

    def _directed_bulk_degrees(self, nbunch, endpoint_key):
        if nbunch is not None and not isinstance(nbunch, (list, tuple)):
            if endpoint_key == self._edge_target_key:
                return super().in_degree(nbunch)
            return super().out_degree(nbunch)

        requested = None if nbunch is None else list(nbunch)
        degrees = {} if requested is None else {node: 0 for node in requested}
        requested_by_id = (
            None if requested is None else {str(node): node for node in requested}
        )
        for edge in self._scan_table(self._edge_table):
            endpoint = edge[endpoint_key]
            if requested_by_id is None:
                degrees[endpoint] = degrees.get(endpoint, 0) + 1
            elif endpoint in requested_by_id:
                degrees[requested_by_id[endpoint]] += 1
        return degrees

    # Ingesting

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> dict:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        # Ingest edges first:

        edge_column_names = [
            c for c in edgelist.columns if c not in [source_column, target_column]
        ]
        sources = edgelist[source_column].tolist()
        targets = edgelist[target_column].tolist()
        edge_metadata = (
            edgelist[edge_column_names].to_dict("records")
            if edge_column_names
            else [{} for _ in range(len(edgelist))]
        )

        tic = time.time()
        with self._edge_table.batch_writer() as batch_writer:
            for source, target, metadata in zip(sources, targets, edge_metadata):
                batch_writer.put_item(
                    Item={
                        self._primary_key: edge_identity(source, target),
                        self._edge_source_key: str(source),
                        self._edge_target_key: str(target),
                        **metadata,
                    }
                )
        edge_toc = time.time() - tic

        tic = time.time()
        # Construct a unique set of nodes:
        nodes = pd.unique(
            pd.concat(
                [edgelist[source_column], edgelist[target_column]],
                ignore_index=True,
            )
        )
        with self._node_table.batch_writer() as batch_writer:
            for node in nodes:
                batch_writer.put_item(Item={self._primary_key: str(node)})

        return {
            "node_count": len(nodes),
            "node_duration": time.time() - tic,
            "edge_count": len(edgelist),
            "edge_duration": edge_toc,
        }
