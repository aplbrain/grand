## *Class* `NetworkXDialect(nx.Graph)`


A NetworkXDialect provides a networkx-like interface for graph manipulation



## *Function* `__init__(self, parent: "Graph")`


Create a new dialect to query a backend with NetworkX syntax.

### Arguments
> - **parent** (`Graph`: `None`): The parent Graph object

### Returns
    None



## *Function* `adj(self)`


https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323


## *Function* `_adj(self)`


https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323


## *Function* `pred(self)`


https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323


## *Function* `_pred(self)`


https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py#L323


## *Class* `IGraphDialect(nx.Graph)`


An IGraphDialect provides a python-igraph-like interface



## *Function* `__init__(self, parent: "Graph")`


Create a new dialect to query a backend with Python-IGraph syntax.

### Arguments
> - **parent** (`Graph`: `None`): The parent Graph object

### Returns
    None



## *Class* `NetworkitDialect`


A Networkit-like API for interacting with a Grand graph.

> - **here** (`None`: `None`): https://networkit.github.io/dev-docs/python_api/graph.html

