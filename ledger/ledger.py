import base64
import logging

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.stores.memory_hash_store import MemoryHashStore
from ledger.tree_hasher import TreeHasher
from ledger.merkle_tree import MerkleTree
from ledger.serializers.mapping_serializer import MappingSerializer
from ledger.serializers.json_serializer import JsonSerializer
from ledger.stores.file_store import FileStore
from ledger.stores.text_file_store import TextFileStore
from ledger.immutable_store import ImmutableStore
from ledger.util import F
from ledger.util import ConsistencyVerificationFailed

class Ledger(ImmutableStore):
    def __init__(self, tree: MerkleTree, dataDir: str,
                 serializer: MappingSerializer=None, fileName: str=None):
        """
        :param tree: an implementation of MerkleTree
        :param dataDir: the directory where the transaction log is stored
        :param serializer: an object that can serialize the data before hashing
        it and storing it in the MerkleTree
        :param fileName: the name of the transaction log file
        """
        self.dataDir = dataDir
        self.tree = tree
        self.leafSerializer = serializer or \
                              JsonSerializer()  # type: MappingSerializer
        # self.preHashingSerializer = JsonSerializer()
        self.hasher = TreeHasher()
        self._transactionLog = None  # type: FileStore
        self._transactionLogName = fileName or "transactions"
        self.start()
        self.seqNo = 0
        self.recoverTree()

    def recoverTree(self):
        # TODO: Should probably have 2 classes of hash store,
        # persistent and non persistent

        # TODO: this definitely should be done in a more generic way:
        if not isinstance(self.tree, CompactMerkleTree):
            logging.error("Do not know how to recover {}".format(self.tree))
            raise TypeError("Merkle tree type {} is not supported"
                            .format(type(self.tree)))


        # ATTENTION!
        # This functionality is disabled until better consistency verification
        # implemented - always using recovery from transaction log

        # if not self.tree.hashStore \
        #         or isinstance(self.tree.hashStore, MemoryHashStore) \
        #         or self.tree.leafCount == 0:
        #     logging.debug("Recovering tree from transaction log")
        #     self.recoverTreeFromTxnLog()
        # else:
        #     try:
        #         logging.debug("Recovering tree from hash store of size {}".
        #                       format(self.tree.leafCount))
        #         self.recoverTreeFromHashStore()
        #     except ConsistencyVerificationFailed:
        #         logging.error("Consistency verification of merkle tree "
        #                       "from hash store failed, "
        #                       "falling back to transaction log")
        #         self.recoverTreeFromTxnLog()

        logging.debug("Recovering tree from transaction log")
        self.recoverTreeFromTxnLog()


    def recoverTreeFromTxnLog(self):
        self.tree.hashStore.reset()
        for key, entry in self._transactionLog.iterator():
            record = self.leafSerializer.deserialize(entry)
            self._addToTree(record)

    def recoverTreeFromHashStore(self):
        # To avoid out of memory reading this file not fully
        numOfTransations = sum((1 for _ in self._transactionLog.iterator()))

        treeSize = self.tree.leafCount
        self.seqNo = treeSize
        hashes = list(reversed(self.tree.inclusion_proof(treeSize, treeSize + 1)))
        self.tree._update(self.tree.leafCount, hashes)

        self.tree.verifyConsistency(numOfTransations)

    def add(self, leaf):
        leafData = self._addToTree(leaf)
        self._addToStore(leaf)
        return leafData

    def _addToTree(self, leafData):
        serializedLeafData = self.serializeLeaf(leafData)
        auditPath = self.tree.append(serializedLeafData)
        self.seqNo += 1
        merkleInfo = {
            F.seqNo.name: self.seqNo,
            F.rootHash.name: base64.b64encode(self.tree.root_hash).decode(),
            F.auditPath.name: [base64.b64encode(h).decode() for h in auditPath]
        }
        return merkleInfo

    def _addToStore(self, data):
        key = str(self.seqNo)
        self._transactionLog.put(key=key,
                                 value=self.leafSerializer.serialize(
                                     data, toBytes=False))

    def append(self, txn):
        merkleInfo = self.add(txn)
        return merkleInfo

    def get(self, **kwargs):
        for seqNo, value in self._transactionLog.iterator():
            data = self.leafSerializer.deserialize(value)
            # If `kwargs` is a subset of `data`
            if set(kwargs.values()) == {data.get(k) for k in kwargs.keys()}:
                data[F.seqNo.name] = int(seqNo)
                return data

    def getBySeqNo(self, seqNo):
        key = str(seqNo)
        value = self._transactionLog.get(key)
        if value:
            return self.leafSerializer.deserialize(value)
        else:
            return value

    def lastCount(self):
        key = self._transactionLog.lastKey
        return 0 if key is None else int(key)

    def serializeLeaf(self, leafData):
        return self.leafSerializer.serialize(leafData)

    @property
    def size(self) -> int:
        return self.tree.tree_size

    @property
    def root_hash(self) -> str:
        return base64.b64encode(self.tree.root_hash).decode()

    def merkleInfo(self, seqNo):
        rootHash = self.tree.merkle_tree_hash(0, int(seqNo))
        auditPath = self.tree.inclusion_proof(0, int(seqNo))
        return {
            F.rootHash.name: base64.b64encode(rootHash).decode(),
            F.auditPath.name: [base64.b64encode(h).decode() for h in auditPath]
        }

    def start(self, loop=None):
        if self._transactionLog and not self._transactionLog.closed:
            logging.debug("Ledger already started.")
        else:
            logging.debug("Starting ledger...")
            self._transactionLog = TextFileStore(self.dataDir,
                                                 self._transactionLogName,
                                                 isLineNoKey=True)

    def stop(self):
        self._transactionLog.close()

    def reset(self):
        self._transactionLog.reset()

    def getAllTxn(self, frm: int=None, to: int=None):
        result = {}
        for seqNo, txn in self._transactionLog.iterator():
            seqNo = int(seqNo)
            if (frm is None or seqNo >= frm) and \
                    (to is None or seqNo <= to):
                result[seqNo] = self.leafSerializer.deserialize(txn)
            if to is not None and seqNo > to:
                break
        return result
