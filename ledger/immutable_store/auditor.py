from ledger.immutable_store.merkle_tree import MerkleTree


class Auditor:
    """
    An object that can check the proof of existence of an arbitrary event
    """
    def audit(self, event, tree: MerkleTree):
        raise NotImplementedError
