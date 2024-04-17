import cachetools.func
from typing import Callable, Hashable, Collection
import abc

import pandas as pd


class Backend(abc.ABC):
    """
    Abstract base class for the management of persisted graph structure.

    Do not use this class directly.

    """

    def __init__(self, directed: bool = False):
        """
        Create a new Backend instance.

        Arguments:
            directed (bool: False): Whether to make the backend graph directed

        Returns:
            None

        """
        ...

    def ingest_from_edgelist_dataframe(
        self, edgelist: pd.DataFrame, source_column: str, target_column: str
    ) -> None:
        """
        Ingest an edgelist from a Pandas DataFrame.

        """
        ...

    def is_directed(self) -> bool:
        """
        Return True if the backend graph is directed.

        Arguments:
            None

        Returns:
            bool: True if the backend graph is directed.

        """
        ...

    def add_node(self, node_name: Hashable, metadata: dict):
        """
        Add a new node to the graph.

        Arguments:
            node_name (Hashable): The ID of the node
            metadata (dict: None): An optional dictionary of metadata
            upsert (bool: True): Update the node if it already exists. If this
                is set to False and the node already exists, a backend may
                choose to throw an error or proceed gracefully.

        Returns:
            Hashable: The ID of this node, as inserted

        """
        ...

    def add_nodes_from(self, nodes_for_adding, **attr):
        """
        Add nodes to the graph.

        Arguments:
            nodes_for_adding: nodes to add
            attr: additional attributes
        """
        for node, metadata in nodes_for_adding:
            self.add_node(node, {**attr, **metadata})

    def get_node_by_id(self, node_name: Hashable):
        """
        Return the data associated with a node.

        Arguments:
            node_name (Hashable): The node ID to look up

        Returns:
            dict: The metadata associated with this node

        """
        ...

    def all_nodes_as_iterable(self, include_metadata: bool = False) -> Collection:
        """
        Get a generator of all of the nodes in this graph.

        Arguments:
            include_metadata (bool: False): Whether to include node metadata in
                the response

        Returns:
            Generator: A generator of all nodes (arbitrary sort)

        """
        ...

    def has_node(self, u: Hashable) -> bool:
        """
        Return true if the node exists in the graph.

        Arguments:
            u (Hashable): The ID of the node to check

        Returns:
            bool: True if the node exists
        """
        try:
            return self.get_node_by_id(u) is not None
        except KeyError:
            return False

    def has_edge(self, u: Hashable, v: Hashable) -> bool:
        """
        Return true if the edge exists in the graph.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            bool: True if the edge exists
        """
        try:
            return self.get_edge_by_id(u, v) is not None
        except KeyError:
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
        ...

    def add_edges_from(self, ebunch_to_add, **attr):
        """
        Add new edges to the graph.

        Arguments:
            ebunch_to_add: list of (source, target, metadata)
            attr: additional common attributes
        """
        for u, v, metadata in ebunch_to_add:
            self.add_edge(u, v, {**attr, **metadata})

    def all_edges_as_iterable(self, include_metadata: bool = False) -> Collection:
        """
        Get a list of all edges in this graph, arbitrary sort.

        Arguments:
            include_metadata (bool: False): Whether to include edge metadata

        Returns:
            Generator: A generator of all edges (arbitrary sort)

        """
        ...

    def get_edge_by_id(self, u: Hashable, v: Hashable):
        """
        Get an edge by its source and target IDs.

        Arguments:
            u (Hashable): The source node ID
            v (Hashable): The target node ID

        Returns:
            dict: Metadata associated with this edge

        """
        ...

    def get_node_successors(
        self, u: Hashable, include_metadata: bool = False
    ) -> Collection:
        return self.get_node_neighbors(u, include_metadata)

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
        ...

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
        ...

    def get_node_count(self) -> int:
        """
        Get an integer count of the number of nodes in this graph.

        Arguments:
            None

        Returns:
            int: The count of nodes

        """
        return len([i for i in self.all_nodes_as_iterable()])

    def get_edge_count(self) -> int:
        """
        Get an integer count of the number of edges in this graph.

        Arguments:
            None

        Returns:
            int: The count of edges

        """
        return len([i for i in self.all_edges_as_iterable()])

    def degree(self, u: Hashable) -> int:
        """
        Get the degree of a node.

        Arguments:
            u (Hashable): The node ID

        Returns:
            int: The degree of the node

        """
        return len([i for i in self.get_node_neighbors(u)])

    def degrees(self, nbunch=None) -> Collection:
        return {
            node: self.degree(node) for node in (nbunch or self.all_nodes_as_iterable())
        }

    def in_degree(self, u: Hashable) -> int:
        """
        Get the in-degree of a node.

        Arguments:
            u (Hashable): The node ID

        Returns:
            int: The in-degree of the node

        """
        return len(list(self.get_node_predecessors(u)))

    def in_degrees(self, nbunch=None) -> Collection:
        nbunch = nbunch or self.all_nodes_as_iterable()
        if isinstance(nbunch, (list, tuple)):
            return {node: self.in_degree(node) for node in nbunch}
        else:
            return self.in_degree(nbunch)

    def out_degree(self, u: Hashable) -> int:
        """
        Get the out-degree of a node.

        Arguments:
            u (Hashable): The node ID

        Returns:
            int: The out-degree of the node

        """
        return len(list(self.get_node_successors(u)))

    def out_degrees(self, nbunch=None) -> Collection:
        nbunch = nbunch or self.all_nodes_as_iterable()
        if isinstance(nbunch, (list, tuple)):
            return {node: self.out_degree(node) for node in nbunch}
        else:
            return self.out_degree(nbunch)


class CachedBackend(Backend):
    """
    A proxy Backend that serves as a cache for any other grand.Backend.

    """

    def __init__(self, backend: Backend): ...


class InMemoryCachedBackend(CachedBackend):
    """
    A proxy Backend that serves as a cache for any other grand.Backend.

    Wraps each call to the Backend with an LRU cache.

    """

    _cache_types = {
        "LRUCache": cachetools.func.lru_cache,
        "TTLCache": cachetools.func.ttl_cache,
        "LFUCache": cachetools.func.lfu_cache,
    }

    _default_uncacheable_methods = [
        "add_node",
        "add_nodes_from",
        "add_edge",
        "add_edges_from",
        "ingest_from_edgelist_dataframe",
    ]

    _default_write_methods = [
        "add_node",
        "add_nodes_from",
        "add_edge",
        "add_edges_from",
        "ingest_from_edgelist_dataframe",
    ]

    def __init__(
        self,
        backend: Backend,
        dirty_cache_on_write: bool = True,
        cache_type: str = "TTLCache",
        uncacheable_methods: list = None,
        write_methods: list = None,
        **cache_kwargs,
    ):
        """
        Initialize a new in-memory cache, using the cachetools library.

        Arguments:
            backend (grand.Backend): The backend to cache
            dirty_cache_on_write (bool): Whether to clear the cache on writes
            cache_type (str: "TTLCache"): The cache type to use. One of
                ["LRUCache", "TTLCache"]
            **cache_kwargs: Additional arguments to pass to the cache


        """
        self.backend = backend
        self._dirty_cache_on_write = dirty_cache_on_write
        self._uncacheable_methods = (
            uncacheable_methods or self._default_uncacheable_methods
        )
        self._write_methods = write_methods or self._default_write_methods
        if cache_type not in self._cache_types:
            raise ValueError(
                f"Unknown cache type: {cache_type}. "
                f"Valid types are: {self._cache_types.keys()}"
            )
        self._cache_factory = lambda: self._cache_types[cache_type](**cache_kwargs)

        self._method_lookup = {}

        def _dirty_cache_decorator(method_: Callable):
            def dirty_cache_dec_wrapper(*args, **kwargs):
                self.clear_cache()
                return method_(*args, **kwargs)

            return dirty_cache_dec_wrapper

        method_list = [
            attribute
            for attribute in dir(self.backend)
            if callable(getattr(self.backend, attribute))
            and not attribute.startswith("_")
        ]
        for method in method_list:
            if method in self._uncacheable_methods:
                setattr(self, method, getattr(self.backend, method))
            else:
                wrapped = self._wrapped(method)
                self._method_lookup[method] = wrapped
                setattr(self, method, wrapped)

            if self._dirty_cache_on_write and method in self._write_methods:
                setattr(
                    self, method, _dirty_cache_decorator(getattr(self.backend, method))
                )

    def _wrapped(self, method: str) -> Callable:
        c = self._cache_factory()(getattr(self.backend, method))
        return c

    def clear_cache(self):
        """
        Clear the cache.

        """
        for _, method in self._method_lookup.items():
            method.cache_clear()

    def cache_info(self):
        return {
            method_name: method.cache_info()
            for method_name, method in self._method_lookup.items()
        }
