from abc import abstractmethod, abstractproperty, ABCMeta
from enum import Enum


class F(Enum):
    client_id = 1
    request_id = 2
    STH = 3
    leaf_data = 4
    leaf_data_hash = 5
    created = 6
    added_to_tree = 7
    audit_info = 8
    serial_no = 9
    tree_size = 10
    root_hash = 11


class ImmutableStore:
    """
    Interface for immutable stores.
    An immutable store is any storage system (database, flatfile, in-memory,
    etc.). It stores the transaction data and the relevant info from the
    Merkle Tree.
    """

    def start(self, loop):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    async def append(self, identifier: str, reply, txnId: str):
        raise NotImplementedError()

    async def get(self, identifier: str, reqId: int):
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()
