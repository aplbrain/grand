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

from ._dynamodb import DynamoDBBackend  # noqa: E402


@pytest.fixture
def backend():
    backend = DynamoDBBackend.__new__(DynamoDBBackend)
    backend._primary_key = "ID"
    backend._node_table = Mock()
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
