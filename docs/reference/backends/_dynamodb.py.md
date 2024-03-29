## *Function* `_dynamo_table_exists(table_name: str, client: boto3.client)`


Check to see if the DynamoDB table already exists.

### Returns
> - **bool** (`None`: `None`): Whether table exists



## *Class* `DynamoDBBackend(Backend)`


A graph datastore that uses DynamoDB for persistance and queries.



## *Function* `is_directed(self) -> bool`


Return True if the backend graph is directed.

### Arguments
    None

### Returns
> - **bool** (`None`: `None`): True if the backend graph is directed.



## *Function* `teardown(self, yes_i_am_sure: bool = False)`


Tear down this graph, deleting all evidence it once was here.



## *Function* `add_node(self, node_name: Hashable, metadata: dict) -> Hashable`


Add a new node to the graph.

Insert a new document into the nodes table.

### Arguments
> - **node_name** (`Hashable`: `None`): The ID of the node
> - **metadata** (`dict`: `None`): An optional dictionary of metadata

### Returns
> - **Hashable** (`None`: `None`): The ID of this node, as inserted



## *Function* `all_nodes_as_iterable(self, include_metadata: bool = False) -> Collection`


Get a generator of all of the nodes in this graph.

### Arguments
> - **include_metadata** (`bool`: `False`): Whether to include node metadata in
        the response

### Returns
> - **Generator** (`None`: `None`): A generator of all nodes (arbitrary sort)



## *Function* `has_node(self, u: Hashable) -> bool`


Return true if the node exists in the graph.

### Arguments
> - **u** (`Hashable`: `None`): The ID of the node to check

### Returns
> - **bool** (`None`: `None`): True if the node exists


## *Function* `add_edge(self, u: Hashable, v: Hashable, metadata: dict)`


Add a new edge to the graph between two nodes.

If the graph is directed, this edge will start (source) at the `u` node and end (target) at the `v` node.

### Arguments
> - **u** (`Hashable`: `None`): The source node ID
> - **v** (`Hashable`: `None`): The target node ID
> - **metadata** (`dict`: `None`): Optional metadata to associate with the edge

### Returns
> - **Hashable** (`None`: `None`): The edge ID, as inserted.



## *Function* `all_edges_as_iterable(self, include_metadata: bool = False) -> Collection`


Get a list of all edges in this graph, arbitrary sort.

### Arguments
> - **include_metadata** (`bool`: `False`): Whether to include edge metadata

### Returns
> - **Generator** (`None`: `None`): A generator of all edges (arbitrary sort)



## *Function* `get_node_by_id(self, node_name: Hashable)`


Return the data associated with a node.

### Arguments
> - **node_name** (`Hashable`: `None`): The node ID to look up

### Returns
> - **dict** (`None`: `None`): The metadata associated with this node



## *Function* `get_edge_by_id(self, u: Hashable, v: Hashable)`


Get an edge by its source and target IDs.

### Arguments
> - **u** (`Hashable`: `None`): The source node ID
> - **v** (`Hashable`: `None`): The target node ID

### Returns
> - **dict** (`None`: `None`): Metadata associated with this edge



## *Function* `get_node_count(self) -> int`


Get an integer count of the number of nodes in this graph.

### Arguments
    None

### Returns
> - **int** (`None`: `None`): The count of nodes

