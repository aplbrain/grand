<div align=center><img src="docs/grand.png" width=400 /></div>

_Grand_ is the Rosetta Stone of graph technologies.

## Example use-cases

-   True-serverless graph databases using DynamoDB\*
-   Pretend NetworkX is Neo4j and use Cypher to query a graph in memory

\* Neptune will not be a true-serverless database until you can pay for data IO only. (Right now, you still need to pay for the virtual-machine CPU hours you're going to spend.)

## Why it's a big deal

_Grand_ is the Rosetta Stone of graph technologies. In short, a _Grand_ graph has a "Backend," which handles the nitty-gritty of talking to data on disk (or in the cloud), and an "Dialect", which is your preferred way of talking to a graph.

For example, here's how you make a graph that is persisted in DynamoDB (the "Backend") but that you can talk to as though it's a `networkx.DiGraph` (the "Dialect"):

```python
import grand

G = grand.Graph(backend=grand.DynamoDBBackend())

G.nx.add_node("Jordan", type="Person")
G.nx.add_node("Steelblue", type="Color")

G.nx.add_edge("Jordan", "Steelblue", type="FavoriteColor")

assert len(G.nx.edges()) == 1
assert len(G.nx.nodes()) == 2
```
