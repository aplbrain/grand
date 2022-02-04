# Backend Caches

When a graph changes little or not at all, it may be desirable to cache the results of backend method calls to be reused on subsequent calls. This is especially useful when the backend is a remote service, such as a database or a REST API, where the cost of each method call may be substantial.

To address this, we recommend the use of a class that inherits from `CachedBackend`. This class will cache the results of backend method calls, and will return the cached results if the method is called again with the same arguments.

Furthermore, this cache will listen for calls to methods (such as `add_node`) that change the graph, and will automatically dirty the cache to avoid returning invalid results.

## Example Usage

```python
from grand.backends import SQLBackend, InMemoryCachedBackend
from grand import Graph

G = Graph(backend=InMemoryCachedBackend(SQLBackend(...)))

G.nx.degree(42) # May be slow if the graph is very large
G.nx.degree(42) # Will be very fast, since the result is cached
G.backend.clear_cache() # Clear the cache explicitly
```
