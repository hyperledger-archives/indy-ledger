from abc import abstractmethod
from typing import List, Tuple


class MerkleTree:
    """
    Interface to be implemented by all Merkle Trees.
    """

    # TODO Check if @property annotation works with interfaces in Python.
    # A lot of these could be properties.
    def append(self, *events):
        """

        :param events:
        :return:
        """

    @abstractmethod
    def getMTH(self):
        """
        """

    @abstractmethod
    def getLastEventTimestamp(self):
        """
        """

    @abstractmethod
    def getAuditPath(self, event):
        """
        """

    @abstractmethod
    def getProof(self, sth1, sth2):
        """
        """
        # TODO Make the default value of the second STH equal the current STH

    # TODO This should probably go into the Monitor if we decide to have one.
    @abstractmethod
    def validate(self, event):
        """
        """


class PersistentMerkleTree(MerkleTree):
    """
    This trait allows Merkle Trees to be written to disk and retrieved in an
    efficient manner.
    Must have a way to index each data entry(i.e leaf) in a Merkle Tree.
    Each leaf would be represented as a key value pair.
    """
    # TODO Can the key simply be a hash of the data in the leaf? This would mean
    # that there won't be any duplicate data in the tree. Is that acceptable?

    @abstractmethod
    def persistenceStore(self):
        """

        :return:
        """

    @abstractmethod
    def store(self, entries: List[Tuple]):
        """

        :param entries:
        :return:
        """

    @abstractmethod
    def retrieve(self, keys: List[str]):
        """

        :param keys:
        :return:
        """

    @abstractmethod
    def load(self):
        """

        :return:
        """
