from typing import Hashable, Generator, Optional
import time

import boto3

from .backend import Backend


_DEFAULT_DYNAMODB_URL = "http://localhost:8000"


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
        node_table_name: str = None,
        edge_table_name: str = None,
        dynamodb_url: str = _DEFAULT_DYNAMODB_URL,
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
        self._node_table_name = node_table_name or "grand_Nodes"
        self._edge_table_name = edge_table_name or "grand_Edges"

        self._primary_key = primary_key
        self._edge_source_key = "Source"
        self._edge_target_key = "Target"

        self._resource = boto3.resource(
            "dynamodb",
            endpoint_url=dynamodb_url,
            aws_access_key_id="",
            aws_secret_access_key="",
        )
        self._client = boto3.client(
            "dynamodb",
            endpoint_url=dynamodb_url,
            aws_access_key_id="",
            aws_secret_access_key="",
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

    def _depaginate_table(self, table):
        done = False
        start_key = None
        results = []
        scan_kwargs = {}
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = table.scan(**scan_kwargs)
            results += response.get("Items", [])
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        return results

    def all_nodes_as_generator(self, include_metadata: bool = False) -> Generator:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        return self._depaginate_table(self._node_table)

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
        response = self._edge_table.put_item(Item=metadata)

        return response

    def all_edges_as_generator(self, include_metadata: bool = False) -> Generator:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        return self._depaginate_table(self._edge_table)

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        response = self._node_table.get_item(Key={self._primary_key: node_name})
        print(response)

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
        print(response)
        item = response["Item"]
        item.pop(self._primary_key)
        item.pop(self._edge_source_key)
        item.pop(self._edge_target_key)
        return item
