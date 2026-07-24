import importlib
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest


if importlib.util.find_spec("boto3") is None:
    boto3 = ModuleType("boto3")
    boto3.client = Mock()
    boto3.resource = Mock()
    conditions = ModuleType("boto3.dynamodb.conditions")
    conditions.Key = Mock()
    sys.modules["boto3"] = boto3
    sys.modules["boto3.dynamodb"] = ModuleType("boto3.dynamodb")
    sys.modules["boto3.dynamodb.conditions"] = conditions

from ._dynamodb import (  # noqa: E402
    _EDGE_SOURCE_INDEX,
    _EDGE_TARGET_INDEX,
    DynamoDBBackend,
    _create_dynamo_table,
)


@pytest.fixture
def backend():
    backend = DynamoDBBackend.__new__(DynamoDBBackend)
    backend._primary_key = "ID"
    backend._edge_source_key = "Source"
    backend._edge_target_key = "Target"
    backend._directed = True
    backend._node_table = Mock()
    backend._edge_table = Mock()
    return backend


def test_has_node_returns_true_when_item_exists(backend):
    backend._node_table.get_item.return_value = {"Item": {"ID": "42"}}

    assert backend.has_node(42)
    backend._node_table.get_item.assert_called_once_with(Key={"ID": "42"})


def test_has_node_returns_false_when_item_is_missing(backend):
    backend._node_table.get_item.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }

    assert not backend.has_node("missing")
    backend._node_table.get_item.assert_called_once_with(Key={"ID": "missing"})


def test_has_node_propagates_table_errors(backend):
    error = RuntimeError("DynamoDB unavailable")
    backend._node_table.get_item.side_effect = error

    with pytest.raises(RuntimeError, match="DynamoDB unavailable"):
        backend.has_node("node")


def test_edge_table_creation_adds_adjacency_indexes():
    resource = Mock()

    _create_dynamo_table(
        "edges",
        "ID",
        resource,
        index_keys=("Source", "Target"),
    )

    kwargs = resource.create_table.call_args.kwargs
    assert kwargs["AttributeDefinitions"] == [
        {"AttributeName": "ID", "AttributeType": "S"},
        {"AttributeName": "Source", "AttributeType": "S"},
        {"AttributeName": "Target", "AttributeType": "S"},
    ]
    assert kwargs["GlobalSecondaryIndexes"] == [
        {
            "IndexName": _EDGE_SOURCE_INDEX,
            "KeySchema": [{"AttributeName": "Source", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        },
        {
            "IndexName": _EDGE_TARGET_INDEX,
            "KeySchema": [{"AttributeName": "Target", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        },
    ]


def test_directed_neighbors_query_source_index_with_pagination(backend):
    backend._edge_table.query.side_effect = [
        {
            "Items": [{"ID": "ab", "Source": "A", "Target": "B"}],
            "LastEvaluatedKey": {"ID": "ab"},
        },
        {"Items": [{"ID": "ac", "Source": "A", "Target": "C"}]},
    ]

    assert set(backend.get_node_neighbors("A")) == {"B", "C"}
    first_query, second_query = backend._edge_table.query.call_args_list
    assert first_query.kwargs["IndexName"] == _EDGE_SOURCE_INDEX
    assert "FilterExpression" not in first_query.kwargs
    assert second_query.kwargs["ExclusiveStartKey"] == {"ID": "ab"}


def test_directed_predecessors_query_target_index(backend):
    backend._edge_table.query.return_value = {
        "Items": [{"ID": "ab", "Source": "A", "Target": "B"}]
    }

    assert list(backend.get_node_predecessors("B")) == ["A"]
    assert backend._edge_table.query.call_args.kwargs["IndexName"] == (
        _EDGE_TARGET_INDEX
    )


def test_undirected_neighbors_query_both_indexes_and_deduplicate(backend):
    backend._directed = False
    edge = {"ID": "aa", "Source": "A", "Target": "A"}
    backend._edge_table.query.side_effect = [
        {"Items": [edge, {"ID": "ab", "Source": "A", "Target": "B"}]},
        {"Items": [edge, {"ID": "ca", "Source": "C", "Target": "A"}]},
    ]

    assert list(backend.get_node_neighbors("A")) == ["A", "B", "C"]
    assert [item.kwargs["IndexName"] for item in backend._edge_table.query.call_args_list] == [
        _EDGE_SOURCE_INDEX,
        _EDGE_TARGET_INDEX,
    ]
    assert backend._edge_table.scan.call_args_list == []
