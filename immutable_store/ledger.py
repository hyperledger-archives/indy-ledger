from immutable_store.error import GeneralMissingError
from immutable_store.merkle import TreeHasher
from immutable_store.merkle_tree import MerkleTree
from immutable_store.store import ImmutableStore, Properties


class Ledger:

    def __init__(self, tree: MerkleTree, store: ImmutableStore):
        """
        :param tree: an implementation of MerkleTree used to store events
        """
        # TODO The initialization logic should take care of migrating the
        # persisted data into a newly created Merkle Tree after server restart.
        self.tree = tree
        self.store = store
        self.hasher = TreeHasher()

        self.recoverTree()

    def recoverTree(self):
        for entry in self.store.getAllTxns():
            self._addTxnToTree(entry)

    def addTxn(self, data):
        self._addTxnToTree(data)
        self._addTxnToStore(data)

    # TODO This currently works for a tree of size 1 only.
    def _addTxnToTree(self, data):
        leaf_data_hash = data[Properties.leaf_data_hash.name]
        leaf_data = data[Properties.leaf_data.name]
        if leaf_data_hash:
            self.tree.append(leaf_data_hash)
        elif leaf_data:
            leaf_hash = self.hasher.hash_leaf(bytes(str(leaf_data), 'utf-8'))
            self.tree.append(leaf_hash)
            leaf_data_hash = leaf_hash
        else:
            raise GeneralMissingError("Transaction not found.")

    def _addTxnToStore(self, data):
        self.store.append(data)


