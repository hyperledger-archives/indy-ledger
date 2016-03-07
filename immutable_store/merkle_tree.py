from typing import List, Tuple


class MerkleTree:
    """
    Interface to be implemented by all Merkle Trees.
    """

    # TODO Check if @property annotation works with interfaces in Python.
    # A lot of these could be properties.
    def insert(self, *events):
        raise NotImplementedError

    def getMTH(self):
        raise NotImplementedError

    def getLastEventTimestamp(self):
        raise NotImplementedError

    def getAuditPath(self, event):
        raise NotImplementedError

    def getProof(self, sth1, sth2):
        # TODO Make the default value of the second STH equal the current STH
        raise NotImplementedError

    # TODO This should probably go into the Monitor if we decide to have one.
    def validate(self, event):
        raise NotImplementedError


class PersistentMerkleTree(MerkleTree):
    """
    This trait allows Merkle Trees to be written to disk and retrieved in an
    efficient manner.
    Must have a way to index each data entry(i.e leaf) in a Merkle Tree.
    Each leaf would be represented as a key value pair.
    """
    # TODO Can the key simply be a hash of the data in the leaf? This would mean
    # that there won't be any duplicate data in the tree. Is that acceptable?

    @property
    def persistenceStore(self):
        raise NotImplementedError

    def store(self, entries: List[Tuple]):
        raise NotImplementedError

    def retrieve(self, keys: List[str]):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError
