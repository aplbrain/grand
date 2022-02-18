import pytest
from .metadatastore import DictMetadataStore, NodeNameManager


def test_can_create_dict_metadata_store():
    store = DictMetadataStore()
    assert store is not None


def test_can_add_to_dict_metadata_store():
    store = DictMetadataStore()
    store.add_node("a", {"a": 1})
    assert store.get_node("a") == {"a": 1}


def test_can_add_edge():
    store = DictMetadataStore()
    store.add_edge("a", "b", {"b": 2})
    assert store.get_edge("a", "b") == {"b": 2}


def test_cannot_get_invalid_node():
    store = DictMetadataStore()
    with pytest.raises(KeyError):
        store.get_node("a")


def test_can_create_dict_name_manager():
    store = NodeNameManager()
    assert store is not None


def test_can_add_name_manager():
    store = NodeNameManager()
    store.add_node("a", 1)
    assert "a" in store
    assert 1 not in store
    assert store.get_name(1) == "a"


def test_can_reverse_lookup_node_name_manager():
    store = NodeNameManager()
    store.add_node("a", 1)
    assert "a" in store
    assert 1 not in store
    assert store.get_id("a") == 1
