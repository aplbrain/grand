# grand

Grand is a graph database adapter for non-graph databases.

## Example use-cases

True-serverless graph databases using DynamoDB

```python
import grand

G = grand.Graph(backend=grand.DynamoDBBackend())

G.nx.add_node("Jordan", type="Person")
G.nx.add_node("Steelblue", type="Color")

G.nx.add_edge("Jordan", "Steelblue", type="FavoriteColor")


```
