import base64
import logging

from ledger.tree_hasher import TreeHasher
from ledger.merkle_tree import MerkleTree
from ledger.serializers.mapping_serializer import MappingSerializer
from ledger.serializers.json_serializer import JsonSerializer
from ledger.stores.file_store import FileStore
from ledger.stores.text_file_store import ChunkedTextFileStore
from ledger.immutable_store import ImmutableStore
from ledger.util import F


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
        self.preHashingSerializer = JsonSerializer()
        self.hasher = TreeHasher()
        self._transactionLog = None  # type: FileStore
        self._transactionLogName = fileName or "transactions"
        self.start()
        self.seqNo = 0
        self.recoverTree()

    def recoverTree(self):
        for key, entry in self._transactionLog.iterator():
            record = self.leafSerializer.deserialize(entry)
            self._addToTree(record)

    def add(self, leaf):
        leafData = self._addToTree(leaf)
        self._addToStore(leaf)
        return leafData

    def _addToTree(self, leafData):
        serializedLeafData = self.preHashingSerializer.serialize(leafData)
        auditPath = self.tree.append(serializedLeafData)
        self.seqNo += 1
        return {
            F.seqNo.name: self.seqNo,
            F.rootHash.name: base64.b64encode(self.tree.root_hash).decode(),
            F.auditPath.name: [base64.b64encode(h).decode() for h in auditPath]
        }

    def _addToStore(self, data):
        key = str(self.seqNo)
        self._transactionLog.put(key=key,
                                 value=self.leafSerializer.serialize(
                                     data, toBytes=False))

    async def append(self, identifier: str, reply, txnId: str):
        merkleInfo = self.add(reply.result)
        return merkleInfo

    async def get(self, identifier: str, reqId: int):
        for value in self._transactionLog.iterator(includeKey=False):
            data = self.leafSerializer.deserialize(value)
            if data.get("identifier") == identifier \
                    and data.get("reqId") == reqId:
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

    @property
    def size(self) -> int:
        return self.tree.tree_size

    @property
    def root_hash(self) -> str:
        return base64.b64encode(self.tree.root_hash).decode()

    def start(self, loop=None):
        if self._transactionLog:
            logging.debug("Ledger already started.")
        else:
            logging.debug("Starting ledger...")
            self._transactionLog = ChunkedTextFileStore(self.dataDir,
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
            if (frm is None or seqNo >= frm) and (to is None or seqNo <= to):
                result[seqNo] = self.leafSerializer.deserialize(txn)
        return result
