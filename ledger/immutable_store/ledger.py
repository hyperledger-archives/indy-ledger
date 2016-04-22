import base64
import logging
from collections import namedtuple

from ledger.immutable_store.merkle import TreeHasher
from ledger.immutable_store.merkle_tree import MerkleTree
from ledger.immutable_store.store import ImmutableStore, F
from ledger.immutable_store.serializers import MappingSerializer
from ledger.immutable_store.serializers import JsonSerializer
from ledger.immutable_store.stores import TextFileStore

Reply = namedtuple('Reply', ['viewNo', 'reqId', 'result'])


class Ledger(ImmutableStore):
    def __init__(self, tree: MerkleTree, dataDir: str, serializer=None, fileName=None):
        """
        :param tree: an implementation of MerkleTree used to store events
        """
        # TODO The initialization logic should take care of migrating the
        # persisted data into a newly created Merkle Tree after server restart.
        self.dataDir = dataDir
        self.tree = tree
        self.nodeSerializer = serializer or JsonSerializer()  # type: MappingSerializer
        self.leafDataSerializer = JsonSerializer()
        self.hasher = TreeHasher()
        self._transactionLog = None
        self._transactionLogName = fileName or "transactions"
        self._processedReq = None  #  Move the fields in processedReq to transactionLog
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
        self._transactionLog.put(key, self.nodeSerializer.serialize(
            data, toBytes=False))

    async def append(self, identifier: str, reply, txnId: str):
        merkleInfo = self.add(reply.result)
        self.insertProcessedReq(identifier, reply.reqId, self.serialNo)
        return merkleInfo

    async def get(self, identifier: str, reqId: int):
        serialNo = self.getProcessedReq(identifier, reqId)
        if serialNo:
            return self.getBySerialNo(serialNo)
        else:
            return None

    def getBySerialNo(self, serialNo):
        key = str(serialNo)
        value = self._transactionLog.get(key)
        if value:
            return self.nodeSerializer.deserialize(value)
        else:
            return value

    def insertProcessedReq(self, identifier, reqId, serial_no):
        key = "{}-{}".format(identifier, reqId)
        value = str(serial_no)
        self._processedReq.put(key, value)

    def getProcessedReq(self, identifier, reqId):
        key = "{}-{}".format(identifier, reqId)
        serialNo = self._processedReq.get(key)
        if serialNo:
            return serialNo
        else:
            return None

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
            self._processedReq = TextFileStore(self.dataDir, "processedReq")

    def stop(self):
        self._transactionLog.close()
        self._processedReq.close()

    def reset(self):
        self._transactionLog.reset()
        self._processedReq.reset()

    def getAllTxn(self):
        result = {}
        for serialNo, txn in self._transactionLog.iterator():
            result[serialNo] = self.nodeSerializer.deserialize(txn)
        return result
