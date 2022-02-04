# Implementation Notes

<!-- A series of collapsible markdown accordions, using <details> and <summary> -->

<h2>Adding nodes is an upsert operation.</h2>

When you call the `add_node` operation on a grand `backend` object, it will check to see if the node already exists in the database. If it does, it will update the node with the new data. If it does not, it will insert the node into the database. Note that there is no difference visible to the user between these two operations.

If you (as the end-user of this library) want to know whether a node is already present in the graph, you can use the `has_node` operation.

Alternatively, you can call the `add_node` operation with the `upsert` flag set to False.
