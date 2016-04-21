from abc import abstractmethod, abstractproperty, ABCMeta
from enum import Enum


class F(Enum):
    clientId = 1
    requestId = 2
    rootHash = 3
    leafData = 4
    leafDataHash = 5
    created = 6
    addedToTree = 7
    auditPath = 8
    serialNo = 9
    treeSize = 10


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
