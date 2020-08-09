from typing import Hashable, Generator, Optional
import time

import boto3

from .backend import Backend


_DEFAULT_DYNAMODB_URL = "http://localhost:8000"


def _dynamo_table_exists(table_name: str, client: boto3.client):
    existing_tables = client.list_tables()["TableNames"]
    print(existing_tables)
    return table_name in existing_tables


def _create_dynamo_table(
    table_name: str, primary_key: str, client, read_write_units: Optional[int] = None,
):
    if read_write_units is not None:
        raise NotImplementedError("Non-on-demand billing is not currently supported.")

    # try:
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
    # except client.exceptions.ResourceInUseException as e:
    #     print(e)
    #     return False


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
        timeout_for_table_creation: int = 10,  # TODO
    ) -> None:
        self._node_table_name = node_table_name or "grand_Nodes"
        self._edge_table_name = edge_table_name or "grand_Edges"

        self._primary_key = primary_key

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
                self._node_table_name, self._primary_key, self._client
            )
            # Await table creation:
            if node_creation_response:
                node_creation_response.meta.client.get_waiter("table_exists").wait(
                    TableName=self._node_table_name
                )

        if not _dynamo_table_exists(self._edge_table_name, self._client):
            edge_creation_response = _create_dynamo_table(
                self._edge_table_name, self._primary_key, self._client
            )
            # Await table creation:
            if edge_creation_response:
                edge_creation_response.meta.client.get_waiter("table_exists").wait(
                    TableName=self._edge_table_name
                )

        self._node_table = self._resource.Table(self._node_table_name)
        self._edge_table = self._resource.Table(self._edge_table_name)

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

