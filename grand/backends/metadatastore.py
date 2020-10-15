from typing import Hashable


class MetadataStore:
    def add_node(self, node_name: Hashable, metadata: dict) -> Hashable:
        raise NotImplementedError()

    def add_edge(self, u: Hashable, v: Hashable, metadata: dict) -> Hashable:
        raise NotImplementedError()

    def get_node(self, node_name: Hashable) -> dict:
        raise NotImplementedError()

    def get_edge(self, u: Hashable, v: Hashable) -> dict:
        raise NotImplementedError()


class DictMetadataStore(MetadataStore):
    def __init__(self):
        self.ndata = {}
        self.edata = {}

    def add_node(self, node_name: Hashable, metadata: dict) -> Hashable:
        if metadata is None:
            metadata = {}
        self.ndata[node_name] = metadata

    def add_edge(self, u: Hashable, v: Hashable, metadata: dict) -> Hashable:
        if metadata is None:
            metadata = {}
        self.edata[(u, v)] = metadata

    def get_node(self, node_name: Hashable) -> dict:
        return self.ndata[node_name]

    def get_edge(self, u: Hashable, v: Hashable) -> dict:
        return self.edata[(u, v)]


class NodeNameManager:
    def __init__(self):
        self.node_names_by_id = {}
        self.node_ids_by_name = {}

    def add_node(self, name: Hashable, _id: Hashable):
        self.node_names_by_id[_id] = name
        self.node_ids_by_name[name] = _id

    def get_name(self, _id: Hashable) -> Hashable:
        return self.node_names_by_id[_id]

    def get_id(self, name: Hashable) -> Hashable:
        return self.node_ids_by_name[name]

    def __contains__(self, name: Hashable) -> bool:
        return name in self.node_ids_by_name
