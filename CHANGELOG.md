# CHANGELOG

## **0.3.0**

> This version adds support for Gremlin-compatible graph databases, such as AWS Neptune, TinkerPop, Janus, etc, through the `GremlinBackend`.

-   Improvements
    -   Backends
        -   Add `GremlinBackend` to the list of supported backends

## **0.2.0**

> This version adds a new `IGraphBackend` (non-default install). If you have IGraph installed already, you can now import this backend with `from grand.backends.igraph import IGraphBackend`.

-   Improvements
    -   Backends
        -   Add `IGraphBackend` to the list of supported backends

## **0.1.0** (January 19, 2021)

> This version adds dependency-install support to installations with `pip`. (Thanks @Raphtor!)

-   Improvements
    -   Faster edge-indexing support for the SQLBackend, using compound primary keys rather than columnwise lookups
