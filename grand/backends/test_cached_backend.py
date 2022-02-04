import time
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
