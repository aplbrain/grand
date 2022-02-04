from typing import Hashable, Generator, Optional, Iterable
import time
import concurrent.futures

import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key

from .backend import Backend


_DEFAULT_DYNAMODB_URL = "http://localhost:4566"
_N_PARALLEL_REQUESTS = 16


def _dynamo_table_exists(table_name: str, client: boto3.client):
    """
    Check to see if the DynamoDB table already exists.

    Returns:
        bool: Whether table exists

    """
    existing_tables = client.list_tables()["TableNames"]
    return table_name in existing_tables


def _create_dynamo_table(
    table_name: str, primary_key: str, client, read_write_units: Optional[int] = None,
):
    if read_write_units is not None:
        raise NotImplementedError("Non-on-demand billing is not currently supported.")

    return client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": primary_key, "KeyType": "HASH"},  # Partition key
            # {"AttributeName": "title", "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": primary_key, "AttributeType": "S"},
            # {"AttributeName": "title", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
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
                self._edge_table_name, self._primary_key, self._resource
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

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Generator:
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
                node[self._primary_key],
                {k: v for k, v in node.items() if k not in [self._primary_key]},
            )
            if include_metadata
            else node[self._primary_key]
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
        try:
            self._node_table.get_item(u)
            return True
        except:
            return False

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
        metadata[self._primary_key] = f"__{u}__{v}"
        if self._edge_source_key in metadata:
            raise KeyError(
                f"'{self._edge_source_key}' should not be in metadata. I need that for PK!"
            )
        metadata[self._edge_source_key] = u
        if self._edge_target_key in metadata:
            raise KeyError(
                f"'{self._edge_target_key}' should not be in metadata. I need that for PK!"
            )
        metadata[self._edge_target_key] = v

        if not self.has_node(u):
            self._node_table.put_item(Item={self._primary_key: u})
        if not self.has_node(v):
            self._node_table.put_item(Item={self._primary_key: v})

        response = self._edge_table.put_item(Item=metadata)

        return response

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        return [
            (edge[self._edge_source_key], edge[self._edge_target_key], edge)
            if include_metadata
            else (edge[self._edge_source_key], edge[self._edge_target_key])
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
        response = self._edge_table.get_item(Key={self._primary_key: f"__{u}__{v}"})
        item = response["Item"]
        item.pop(self._primary_key)
        item.pop(self._edge_source_key)
        item.pop(self._edge_target_key)
        return item

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
            # Return only edges for which `u` is the source
            res = self._scan_table(
                self._edge_table,
                {"FilterExpression": Key(self._primary_key).begins_with(f"__{u}__"),},
            )

        else:
            res = self._scan_table(
                self._edge_table,
                {
                    "FilterExpression": (
                        Key(self._edge_source_key).eq(u)
                        | Key(self._edge_target_key).eq(u)
                    ),
                },
            )

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
    ) -> Generator:
        """
        Get a generator of all upstream nodes from this node.

        Arguments:
            u (Hashable): The source node ID

        Returns:
            Generator

        """
        if self._directed:
            # Return only edges for which `u` is the target
            res = self._scan_table(
                self._edge_table,
                {"FilterExpression": Key(self._edge_target_key).eq(u),},
            )

        else:
            res = self._scan_table(
                self._edge_table,
                {
                    "FilterExpression": (
                        Key(self._edge_source_key).eq(u)
                        | Key(self._edge_target_key).eq(u)
                    ),
                },
            )

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

    def get_node_count(self) -> Iterable:
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

    # Ingesting

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> None:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        # Ingest edges first:

        edge_column_names = [
            c for c in edgelist.columns if c not in [source_column, target_column]
        ]

        tic = time.time()
        with self._edge_table.batch_writer() as batch_writer:

            with concurrent.futures.ThreadPoolExecutor(
                _N_PARALLEL_REQUESTS
            ) as executor:
                result_futures = list(
                    map(
                        lambda i: executor.submit(
                            lambda: batch_writer.put_item(
                                Item={
                                    self._primary_key: f"__{edgelist._get_value(i, source_column)}__{edgelist._get_value(i, target_column)}",
                                    self._edge_source_key: edgelist._get_value(
                                        i, source_column
                                    ),
                                    self._edge_target_key: edgelist._get_value(
                                        i, target_column
                                    ),
                                    **{
                                        col: edgelist._get_value(i, col)
                                        for col in edge_column_names
                                    },
                                }
                            )
                        ),
                        edgelist.index,
                    )
                )
                for future in concurrent.futures.as_completed(result_futures):
                    future.result()

            # for edge in edges:
            #     batch_writer.put_item(Item=edge)
        edge_toc = time.time() - tic

        tic = time.time()
        # Construct a unique set of nodes:
        nodes = edgelist[source_column].append(edgelist[target_column]).unique()
        with self._node_table.batch_writer() as batch_writer:
            with concurrent.futures.ThreadPoolExecutor(
                _N_PARALLEL_REQUESTS
            ) as executor:
                result_futures = list(
                    map(
                        lambda x: executor.submit(
                            lambda: batch_writer.put_item(
                                Item={self._primary_key: str(x)}
                            )
                        ),
                        nodes,
                    )
                )
                for future in concurrent.futures.as_completed(result_futures):
                    future.result()

        return {
            "node_count": len(nodes),
            "node_duration": time.time() - tic,
            "edge_count": len(edgelist),
            "edge_duration": edge_toc,
        }
