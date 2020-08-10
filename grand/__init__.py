"""
Grand graph databasifier.

Aug 2020
"""

# docker run -p 8000:8000 amazon/dynamodb-local

import abc
from typing import Hashable, Generator

import networkx as nx

from .backends import Backend, NetworkXBackend
from .dialects import NetworkXDialect, IGraphDialect, CypherDialect


_DEFAULT_BACKEND = NetworkXBackend


class Graph:
    """
    A grand.Graph enables you to manipulate a graph using multiple dialects.

    """

    def __init__(self, backend: Backend = None):
        """
        Create a new grand.Graph.

        Arguments:
            backend (Backend): The backend to use. If none is provided, will
                default to _DEFAULT_BACKEND.

        Returns:
            None

        """
        self.backend = backend or _DEFAULT_BACKEND()

        # Attach dialects:
        self.nx = NetworkXDialect(self)
        self.igraph = IGraphDialect(self)

    def save(self, filename: str) -> str:
        raise NotImplementedError()

    @staticmethod
    def load(self, filename: str) -> str:
        raise NotImplementedError()
