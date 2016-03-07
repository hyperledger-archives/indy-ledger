from immutable_store.merkle_tree import MerkleTree


class Ledger:

    def __init__(self, tree: MerkleTree):
        """
        :param tree: an implementation of MerkleTree used to store events
        """
        self.tree = tree
