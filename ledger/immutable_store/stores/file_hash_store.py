from ledger.immutable_store.stores import BinaryFileStore
from ledger.immutable_store.stores.hash_store import HashStore


class FileHashStore(HashStore):
    def __init__(self, dataDir, fileNamePrefix="", leafSize=32, nodeSize=37):
        self.dataDir = dataDir
        self.fileNamePrefix = fileNamePrefix
        nodesFileName = fileNamePrefix + "_merkleNodes"
        leavesFileName = fileNamePrefix + "_merkleLeaves"

        self.nodesFile = BinaryFileStore(self.dataDir, nodesFileName,
                                         keyIsLineNo=True,
                                         storeContentHash=False)
        self.leavesFile = BinaryFileStore(self.dataDir,
                                        leavesFileName,
                                        keyIsLineNo=True,
                                          storeContentHash=False)

        # Do not need line separators since each entry is of fixed size
        self.nodesFile.lineSep = b''
        self.leavesFile.lineSep = b''
        self.nodeSize = nodeSize
        self.leafSize = leafSize

    @staticmethod
    def write(data, store, size):
        if not isinstance(data, bytes):
            data = data.encode()
        dataaSize = len(data)
        if dataaSize != size:
            raise ValueError("Data size not allowed. Size of the data should be "
                             "{} but instead was {}".format(size, dataaSize))
        store.put(value=data)

    @staticmethod
    def read(store, entryNo, size):
        store._dbFile.seek((entryNo-1) * size)
        return store._dbFile.read(size)

    def writeNode(self, node):
        # TODO: Need to have some exception handling around converting to bytes
        # since they can result in `OverflowError`
        start, height, nodeHash = node
        start = start.to_bytes(4, byteorder='little')
        height = height.to_bytes(1, byteorder='little')
        data = start + height + nodeHash
        self.write(data, self.nodesFile, self.nodeSize)

    def writeLeaf(self, leaf):
        self.write(leaf, self.leavesFile, self.leafSize)

    def getNode(self, pos):
        data = self.read(self.nodesFile, pos, self.nodeSize)
        # TODO: Should probably check for file size equal to nodeSize
        if len(data) < self.nodeSize:
            raise ValueError("No node at given position")
        start = int.from_bytes(data[:4], byteorder='little')
        height = int.from_bytes(data[4:5], byteorder='little')
        nodeHash = data[5:]
        return start, height, nodeHash

    def getLeaf(self, pos):
        data = self.read(self.leavesFile, pos, self.leafSize)
        # TODO: Should probably check for file size equal to nodeSize
        if len(data) < self.leafSize:
            raise ValueError("No leaf at given position")
        return data

    def close(self):
        self.nodesFile.close()
        self.leavesFile.close()

    def reset(self):
        self.nodesFile.reset()
        self.leavesFile.reset()

