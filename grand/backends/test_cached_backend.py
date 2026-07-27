import time
import pytest
from .backend import InMemoryCachedBackend
from ._networkx import NetworkXBackend
from ._sqlbackend import SQLBackend


def test_can_create_cached_backend():
    InMemoryCachedBackend(NetworkXBackend(), maxsize=1024, ttl=20)


def test_can_add_node():
    vanilla = NetworkXBackend()
    cached = InMemoryCachedBackend(vanilla, maxsize=1024, ttl=20)
    assert vanilla.get_node_count() == 0
    assert cached.get_node_count() == 0
    cached.add_node("a", {})
    assert vanilla.get_node_count() == 1
    assert cached.get_node_count() == 1


def test_added_node_ignored_when_no_dirty_on_write():
    vanilla = NetworkXBackend()
    cached = InMemoryCachedBackend(
        vanilla, dirty_cache_on_write=False, maxsize=1024, ttl=20
    )
    assert vanilla.get_node_count() == 0
    assert cached.get_node_count() == 0
    cached.add_node("a", {})
    assert vanilla.get_node_count() == 1
    assert cached.get_node_count() == 0


def test_can_add_nodes():
    cached = InMemoryCachedBackend(NetworkXBackend(), maxsize=1024, ttl=20)
    assert cached.get_node_count() == 0
    cached.add_node("a", {})
    assert cached.get_node_count() == 1
    cached.add_node("a", {})
    assert cached.get_node_count() == 1
    cached.add_node("b", {})
    assert cached.get_node_count() == 2


def test_cache_is_faster_than_no_cache():
    cached = InMemoryCachedBackend(SQLBackend(), maxsize=1024, ttl=20)
    for i in range(1000):
        cached.add_node(i, {})

    tic = time.time()
    node_count = cached.get_node_count()
    toc = time.time()

    tic2 = time.time()
    node_count2 = cached.get_node_count()
    toc2 = time.time()

    # Dirty the cache:
    cached.add_node(1000, {})

    tic3 = time.time()
    node_count3 = cached.get_node_count()
    toc3 = time.time()

    assert node_count == node_count2 == (node_count3 - 1)
    assert (toc - tic) > (toc2 - tic2)
    assert (toc3 - tic3) > (toc2 - tic2)


def test_cache_info():
    cached = InMemoryCachedBackend(SQLBackend(), maxsize=1024, ttl=20)
    cached.add_node("foo", {})
    assert cached.cache_info()["get_node_count"].misses == 0
    assert cached.cache_info()["get_node_count"].hits == 0

    cached.get_node_count()

    assert cached.cache_info()["get_node_count"].misses == 1
    assert cached.cache_info()["get_node_count"].hits == 0

    cached.get_node_count()

    assert cached.cache_info()["get_node_count"].misses == 1
    assert cached.cache_info()["get_node_count"].hits == 1


def test_cached_iterators_can_be_consumed_repeatedly():
    vanilla = NetworkXBackend()
    vanilla.add_edge("A", "B", {})
    cached = InMemoryCachedBackend(vanilla, maxsize=1024, ttl=20)

    assert list(cached.get_node_neighbors("A")) == ["B"]
    assert list(cached.get_node_neighbors("A")) == ["B"]


def test_cached_mutable_values_are_isolated_from_callers():
    vanilla = NetworkXBackend()
    vanilla.add_edge("A", "B", {"weight": {"value": 1}})
    cached = InMemoryCachedBackend(vanilla, maxsize=1024, ttl=20)

    metadata = cached.get_edge_by_id("A", "B")
    metadata["weight"]["value"] = 99

    assert cached.get_edge_by_id("A", "B") == {"weight": {"value": 1}}


def test_failed_writes_do_not_clear_cache():
    class FailingBackend(NetworkXBackend):
        def add_node(self, *args, **kwargs):
            raise RuntimeError("write failed")

    vanilla = FailingBackend()
    cached = InMemoryCachedBackend(vanilla, maxsize=1024, ttl=20)
    cached.get_node_count()

    with pytest.raises(RuntimeError, match="write failed"):
        cached.add_node("A", {})

    assert cached.cache_info()["get_node_count"].hits == 0
    cached.get_node_count()
    assert cached.cache_info()["get_node_count"].hits == 1
