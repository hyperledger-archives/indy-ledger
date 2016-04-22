from ledger.immutable_store.stores.hash_store import HashStore


class MemoryHashStore(HashStore):
    def __init__(self):
        self.nodes = []
        self.leafs = []

    def writeLeaf(self, leaf):
        self.leafs.append(leaf)

    def writeNode(self, node):
        self.nodes.append(node)

    def getLeaf(self, pos):
        return self.leafs[pos-1]

    def getNode(self, pos):
        return self.nodes[pos-1]
