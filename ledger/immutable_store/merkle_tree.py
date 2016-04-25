from abc import abstractmethod, abstractproperty
from typing import List, Tuple


class MerkleTree:
    """
    Interface to be implemented by all Merkle Trees.
    """

    @abstractmethod
    def append(self, new_leaf):
        """
        """

    @abstractmethod
    def merkle_tree_hash(self, start, end):
        """
        """

    @abstractmethod
    def consistency_proof(self, first, second):
        """
        """

    @abstractmethod
    def inclusion_proof(self, start, end):
        """
        """

    @abstractmethod
    def get_tree_head(self, seq=None):
        """
        """

    @abstractmethod
    def root_hash(self):
        """
        """

    @abstractproperty
    def tree_size(self):
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
