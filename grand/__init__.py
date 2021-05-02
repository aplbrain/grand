"""
Grand graph databasifier.

Aug 2020
"""

from .backends import Backend, NetworkXBackend
from .dialects import NetworkXDialect, IGraphDialect


_DEFAULT_BACKEND = NetworkXBackend

__version__ = "0.2.0"


class Graph:
    """
    A grand.Graph enables you to manipulate a graph using multiple dialects.

    """

    def __init__(
        self, backend: Backend = None, directed: bool = True, dialects: dict = None
    ):
        """
        Create a new grand.Graph.

        Arguments:
            backend (Backend): The backend to use. If none is provided, will
                default to _DEFAULT_BACKEND.

        Returns:
            None

        """
        self.backend = backend or _DEFAULT_BACKEND(directed=directed)

        # Attach dialects:
        self.nx = NetworkXDialect(self)
        self.igraph = IGraphDialect(self)

        if dialects:
            self.dialects = {k: v(self) for k, v in dialects.items()}

    def save(self, filename: str) -> str:
        raise NotImplementedError()

    @staticmethod
    def load(self, filename: str) -> str:
        raise NotImplementedError()
