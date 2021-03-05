<div align=center><img src="docs/grand.png" width=400 /></div>

_Grand_ lets you write your algorithms in one graph library but run them in another.

## Installation

```shell
pip install grand-graph
```

## Example use-cases

-   Write NetworkX commands to analyze true-serverless graph databases using DynamoDB\*
-   Query a host graph in SQL for subgraph isomorphisms with DotMotif
-   Write iGraph code to construct a graph, and then play with it in Networkit
-   Attach node and edge attributes to Networkit or IGraph graphs

> \* [Neptune is not true-serverless.](docs/What-About-Neptune.md)

## Why it's a big deal

_Grand_ is a Rosetta Stone of graph technologies. A _Grand_ graph has a "Backend," which handles the implementation-details of talking to data on disk (or in the cloud), and an "Dialect", which is your preferred way of talking to a graph.

For example, here's how you make a graph that is persisted in DynamoDB (the "Backend") but that you can talk to as though it's a `networkx.DiGraph` (the "Dialect"):

```python
import grand

G = grand.Graph(backend=grand.DynamoDBBackend())

G.nx.add_node("Jordan", type="Person")
G.nx.add_node("DotMotif", type="Project")

G.nx.add_edge("Jordan", "DotMotif", type="Created")

assert len(G.nx.edges()) == 1
assert len(G.nx.nodes()) == 2
```

It doesn't stop there. If you like the way IGraph handles anonymous node insertion (ugh) but you want to handle the graph using regular NetworkX syntax, use a `IGraphDialect` and then switch to a `NetworkXDialect` halfway through:

```python
import grand

G = grand.Graph()

# Start in igraph:
G.igraph.add_vertices(5)

# A little bit of networkit:
G.networkit.addNode()

# And switch to networkx:
assert len(G.nx.nodes()) == 6

# And back to igraph!
assert len(G.igraph.vs) == 6
```

You should be able to use the "dialect" objects the same way you'd use a real graph from the constituent libraries. For example, here is a NetworkX algorithm running on NetworkX graphs alongside Grand graphs:

```python
import networkx as nx

nx.algorithms.isomorphism.GraphMatcher(networkxGraph, grandGraph.nx)
```

Here is an example of using Networkit, a highly performant graph library, and attaching node/edge attributes, which are not supported by the library by default:

```python
import grand
from grand.backends.networkit import NetworkitBackend

G = grand.Graph(backend=NetworkitBackend())

G.nx.add_node("Jordan", type="Person")
G.nx.add_node("Grand", type="Software")
G.nx.add_edge("Jordan", "Grand", weight=1)

print(G.nx.edges(data=True)) # contains attributes, even though graph is stored in networkit
```

## Current Support

<table><tr>
<th>✅ = Fully Implemented</th>
<th>🤔 = In Progress</th>
<th>🔴 = Unsupported</th>
</tr></table>

| Dialect           | Description & Notes                            | Status |
| ----------------- | ---------------------------------------------- | ------ |
| `CypherDialect`   | Cypher syntax queries                          | 🔴     |
| `DotMotifDialect` | DotMotif subgraph isomorphisms                 | 🤔     |
| `IGraphDialect`   | Python-IGraph interface (no metadata)          | 🤔     |
| `NetworkXDialect` | NetworkX-like interface for graph manipulation | ✅     |
| `NetworkitDialect` | Networkit-like interface (no metadata)        | ✅     |

| Backend           | Description & Notes                                 | Status |
| ----------------- | --------------------------------------------------- | ------ |
| `DynamoDBBackend` | A graph stored in two sister tables in AWS DynamoDB | ✅     |
| `NetworkXBackend` | A NetworkX graph, in memory                         | ✅     |
| `SQLBackend`      | A graph stored in two SQL-queryable sister tables   | ✅     |

You can read more about usage and learn about backends and dialects in [the wiki](https://github.com/aplbrain/grand/wiki).
