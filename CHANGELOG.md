# CHANGELOG

## **0.8.0** (July 28, 2026)

-   Backends:
    -   Make SQL mutations atomic and durable, preserve data during ingestion,
        harden connection lifecycle handling, and fix undirected degree
        aggregation.
    -   Replace DynamoDB adjacency scans with indexed queries, correct node
        existence checks, and batch degree queries.
    -   Add collision-safe persisted edge identities for SQL and DynamoDB.
    -   Repair pandas 2 DataFrame ingestion, edge-only node enumeration, and
        undirected predecessor traversal.
    -   Make Networkit duplicate node updates consistent and return zero density
        for empty and singleton graphs.
    -   Correct Gremlin predecessor traversal, edge metadata retrieval, and edge
        upsert behavior.
    -   Isolate cached iterators and mutable values from cache consumers.
-   Dialects:
    -   Preserve reciprocal directed edges in the NetworkX dialect.
-   Performance:
    -   Improve pandas and igraph ingestion and backend degree operations.
    -   Add CodSpeed benchmarks for real backend operations.
-   Housekeeping:
    -   Add a Gremlin optional dependency and run mocked Gremlin tests in CI.
    -   Expand shared backend contracts and add focused DynamoDB, SQL transaction,
        cache, Gremlin, and performance coverage.

## **0.7.0** (May 25, 2025)

-   Housekeeping:
    -   Upgrade from setup.py to pyproject.toml (#62, thanks @AdrianVollmer!)
-   Bugfixes:
    -   Fix NetworkX `degree` parity (#64)

## **0.6.0** (December 8, 2024)

-   Features:
    -   Upgrade codspeed CI runner to v3
-   Bugfixes:
    -   Fix `get_node_by_id` method in `SQLBackend` (#59. thanks @davidmezzetti!)
    -   Add `remove_node` to the `CachedBackend` (#59. thanks @davidmezzetti!)

## **0.5.3** (December 7, 2024)

-   Features:
    -   Add `remove_node` method to SQLBackend (#58. thanks @davidmezzetti!)
-   Bugfixes:
    -   Modify sqlite backend `has_node` to return a bool vs int (#58. thanks @davidmezzetti!)
    -   Update method parameters to conform to existing naming standards (#58. thanks @davidmezzetti!)

## **0.5.2** (June 4, 2024)

-   Bugfixes:
    -   Fixes SQLBackend mistakenly referencing Nodes table for enumeration of Edges (#55, #56)

## **0.5.1** (May 13, 2024)

-   Bugfixes:
    -   Fixes SQLBackend bug where graphs would not commit down to disk after transactions (#51, thanks @acthecoder23!)

## **0.5.0** (April 17, 2024)

-   Features
    -   https://github.com/aplbrain/grand/pull/45 (thanks @davidmezzetti!)
        -   Improved support for SQL backend, including an index on edge.sources and edge.targets
        -   Improved batch-adding performance for nodes and edges
-   Housekeeping
    -   Removed extra arguments to Graph constructor and improved Graph kwargs handing
    -   Added `grand.DiGraph` convenience wrapper for directed graphs
-   Dialects
    -   Expanded Networkit dialect test coverage
    -   Added support for exporting NetworkX graphs by adding `graph` attribute

## **0.4.2** (May 7, 2022)

-   Backends
    -   NEW: `DataFrameBackend` supports operations on a pandas-like API.
-   Housekeeping
    -   Added tests for metadata stores
    -   Added IGraph and Networkit backends to the standard GitHub Actions CI test suite

## **0.4.1** (February 10, 2022)

-   Improvements
    -   Dialects
        -   NetworkX Dialect:
            -   Edges are now reported as EdgeViews or other NetworkX reporting classes.
-   Housekeeping
    -   Removed unused tests
    -   Removed DotMotif dialect, which is no longer a goal of this repository (and is still possible by passing a grand.Graph#nx to the DotMotif library.)

## **0.4.0**

-   Improvements
    -   Backends
        -   SQLBackend:
            -   Add support for user-specified edge column names, with `edge_table_source_column` and `edge_table_target_column` arguments.
            -   Fix buggy performance when updating nodes and edges.
        -   Caching support. You can now cache the results from backend methods by wrapping with a class that inherits from `CachedBackend`.
-   Housekeeping
    -   Fix `pip install grand-graph[sql]` and `pip install grand-graph[dynamodb]`, which failed on previous versions due to a faulty setup.py key.
    -   Rename all backend files.

## **0.3.0**

> This version adds support for Gremlin-compatible graph databases, such as AWS Neptune, TinkerPop, Janus, etc, through the `GremlinBackend`, and loosens the requirements for the base installation of `grand-graph`. You can now install `grand-graph[sql]` or `grand-graph[dynamodb]` to get additional functionality (with additional dependencies).

-   Improvements
    -   Backends
        -   Add `GremlinBackend` to the list of supported backends
-   Housekeeping
    -   Remove sqlalchemy and boto3 from the list of requirements for the base install. You can now install these with `pip3 install grand-graph[sql]` or `[dyanmodb]`.

## **0.2.0**

> This version adds a new `IGraphBackend` (non-default install). If you have IGraph installed already, you can now import this backend with `from grand.backends.igraph import IGraphBackend`.

-   Improvements
    -   Backends
        -   Add `IGraphBackend` to the list of supported backends

## **0.1.0** (January 19, 2021)

> This version adds dependency-install support to installations with `pip`. (Thanks @Raphtor!)

-   Improvements
    -   Faster edge-indexing support for the SQLBackend, using compound primary keys rather than columnwise lookups
