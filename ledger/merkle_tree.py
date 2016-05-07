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
