import base64
import logging

from ledger.immutable_store.stores import FileStore
from ledger.immutable_store.merkle import TreeHasher
from ledger.immutable_store.merkle_tree import MerkleTree
from ledger.immutable_store.serializers import JsonSerializer
from ledger.immutable_store.serializers import MappingSerializer
from ledger.immutable_store.store import ImmutableStore, F
from ledger.immutable_store.stores import TextFileStore


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
        # TODO The initialization logic should take care of migrating the
        # persisted data into a newly created Merkle Tree after server restart.
        self.dataDir = dataDir
        self.tree = tree
        self.nodeSerializer = serializer or \
                              JsonSerializer()  # type: MappingSerializer
        self.leafDataSerializer = JsonSerializer()
        self.hasher = TreeHasher()
        self._transactionLog = None  # type: FileStore
        self._transactionLogName = fileName or "transactions"
        self.start()
        self.serialNo = self.lastCount()
        self.recoverTree()

    def recoverTree(self):
        for key, entry in self._transactionLog.iterator():
            record = self.nodeSerializer.deserialize(entry)
            self._addToTree(record)

    def add(self, data):
        addedData = self._addToTree(data)
        self._addToStore(data)
        return addedData

    def _addToTree(self, data):
        leaf_data = self.leafDataSerializer.serialize(data)
        auditPath = self.tree.append(leaf_data)
        self.serialNo += 1
        return {
            F.serialNo.name: self.serialNo,
            F.rootHash.name: base64.b64encode(self.tree.root_hash()).decode(),
            F.auditPath.name: [base64.b64encode(h).decode() for h in auditPath]
        }

    def _addToStore(self, data):
        key = str(self.serialNo)
        self._transactionLog.put(key=key, value=self.nodeSerializer.serialize(
            data, toBytes=False))

    async def append(self, identifier: str, reply, txnId: str):
        merkleInfo = self.add(reply.result)
        return merkleInfo

    async def get(self, identifier: str, reqId: int):
        for value in self._transactionLog.iterator(include_key=False):
            data = self.nodeSerializer.deserialize(value)
            # TODO: The ledger should not know about the field name
            if data.get("identifier") == identifier and \
                            data.get("reqId") == reqId:
                return data

    def getBySerialNo(self, serialNo):
        key = str(serialNo)
        value = self._transactionLog.get(key)
        if value:
            return self.nodeSerializer.deserialize(value)
        else:
            return value

    def lastCount(self):
        key = self._transactionLog.lastKey
        return 0 if key is None else int(key)

    @property
    def size(self) -> int:
        return self.tree.tree_size

    def start(self, loop=None):
        if self._transactionLog:
            logging.debug("Ledger already started.")
        else:
            logging.debug("Starting ledger...")
            self._transactionLog = TextFileStore(self.dataDir,
                                                 self._transactionLogName,
                                                 keyIsLineNo=True)

    def stop(self):
        self._transactionLog.close()

    def reset(self):
        self._transactionLog.reset()

    def getAllTxn(self):
        result = {}
        for serialNo, txn in self._transactionLog.iterator():
            result[serialNo] = self.nodeSerializer.deserialize(txn)
        return result
