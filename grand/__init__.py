"""
Grand graphs package.

"""

from typing import Optional
from .backends import Backend, NetworkXBackend
from .dialects import NetworkXDialect, IGraphDialect


_DEFAULT_BACKEND = NetworkXBackend

__version__ = "0.5.0"


class Graph:
    """
    A grand.Graph enables you to manipulate a graph using multiple dialects.

    """

    def __init__(self, backend: Optional[Backend] = None, **backend_kwargs: dict):
        """
        Create a new grand.Graph.

        The only positional argument is the backend to use. All other arguments
        are passed to the backend's constructor, if a type is provided.
        Otherwise, kwargs are ignored.

        Arguments:
            backend (Backend): The backend to use. If none is provided, will
                default to _DEFAULT_BACKEND.

        """
        self.backend = backend or _DEFAULT_BACKEND

        # If you passed a class instead of an instance, instantiate it with
        # kwargs from the constructor:
        if isinstance(self.backend, type):
            self.backend = self.backend(**backend_kwargs)

        # Attach dialects:
        self.nx = NetworkXDialect(self)
        self.igraph = IGraphDialect(self)


class DiGraph(Graph):
    """
    A grand.DiGraph enables you to manipulate a directed graph. This is a
    convenience class that inherits from grand.Graph.

    """

    def __init__(self, backend: Optional[Backend] = None, **backend_kwargs: dict):
        """
        Create a new grand.DiGraph.

        The only positional argument is the backend to use. All other arguments
        are passed to the backend's constructor, if a type is provided.
        Otherwise, kwargs are ignored.

        Arguments:
            backend (Backend): The backend to use. If none is provided, will
                default to _DEFAULT_BACKEND.

        """
        super().__init__(backend, **{**backend_kwargs, "directed": True})
