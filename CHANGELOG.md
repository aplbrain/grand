# CHANGELOG

## **0.5.0** (Unreleased)

-   Housekeeping
    -   Removed extra arguments to Graph constructor and improved Graph kwargs handing
    -   Added `grand.DiGraph` convenience wrapper for directed graphs

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
