import logging
from collections import namedtuple

from ledger.immutable_store.error import GeneralMissingError
from ledger.immutable_store.merkle import TreeHasher
from ledger.immutable_store.merkle_tree import MerkleTree
from ledger.immutable_store.store import ImmutableStore, F
from ledger.immutable_store.serializers import MappingSerializer
from ledger.immutable_store.serializers import JsonSerializer
from ledger.immutable_store.stores import TextFileStore

Reply = namedtuple('Reply', ['viewNo', 'reqId', 'result'])


class Ledger(ImmutableStore):
    def __init__(self, tree: MerkleTree, dataDir: str, serializer=None):
        """
        :param tree: an implementation of MerkleTree used to store events
        """
        # TODO The initialization logic should take care of migrating the
        # persisted data into a newly created Merkle Tree after server restart.
        self.dataDir = dataDir
        self.tree = tree
        self.nodeSerializer = serializer or JsonSerializer() # type: MappingSerializer
        self.leafDataSerializer = JsonSerializer()
        self.hasher = TreeHasher()
        self._db = None
        self._reply = None
        self._processedReq = None
        self.start()
        self.serialNo = self.lastCount() - 1
        self.recoverTree()

    def recoverTree(self):
        for key, entry in self._reply.iterator():
            record = self.nodeSerializer.deserialize(entry)
            self._addToTree(record)

    def add(self, data):
        addedData = self._addToTree(data)
        self._addToStore(addedData)

    def _addToTree(self, data):
        leaf_data_hash = data[F.leaf_data_hash.name]
        leaf_data = data[F.leaf_data.name]
        if leaf_data_hash:
            if isinstance(leaf_data_hash, str):
                leaf_data_hash = leaf_data_hash.encode()
            self.tree.append(leaf_data_hash)
        elif leaf_data:
            leaf_data_hash = self.hasher.hash_leaf(self.leafDataSerializer.serialize(
                leaf_data))
            self.tree.append(leaf_data_hash)
        else:
            raise GeneralMissingError("Transaction not found.")
        data[F.audit_info.name] = self.tree.inclusion_proof(self.serialNo,
                                                       self.tree.tree_size)
        return data

    def _addToStore(self, data):
        serialNo = data[F.serial_no.name]
        key = str(serialNo)
        self._reply.put(key, self.nodeSerializer.serialize(
            data, toBytes=False))

    async def append(self, identifier: str, reply, txnId: str):
        self.serialNo += 1
        data = {
            F.serial_no.name: self.serialNo,
            F.STH.name: self.getSTH(),
            F.leaf_data.name: reply.result,
            F.leaf_data_hash.name: self.hasher.hash_leaf(
                self.leafDataSerializer.serialize(reply.result))
        }
        self.add(data)
        self.insertProcessedReq(identifier, reply.reqId, self.serialNo)

    async def get(self, identifier: str, reqId: int):
        serialNo = self.getProcessedReq(identifier, reqId)
        if serialNo:
            return self.getBySerialNo(serialNo)[F.leaf_data.name]
        else:
            return None

    def getBySerialNo(self, serialNo):
        key = str(serialNo)
        value = self._reply.get(key)
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
        key = self._reply.lastKey
        return 0 if key is None else int(key)

    def getSTH(self):
        return {
            F.tree_size.name: self.size + 1,
            F.root_hash.name: self.tree.root_hash_hex()
        }

    @property
    def size(self) -> int:
        return self.tree.tree_size

    def start(self, loop=None):
        if self._reply or self._processedReq:
            logging.debug("Ledger already started.")
        else:
            logging.debug("Starting ledger...")
            self._reply = TextFileStore(self.dataDir, "reply", keyIsLineNo=True)
            self._processedReq = TextFileStore(self.dataDir, "processedReq")

    def stop(self):
        self._reply.close()
        self._processedReq.close()

    def reset(self):
        self._reply.reset()
        self._processedReq.reset()

    def getAllTxn(self):
        result = {}
        for txnId, reply in self._reply.iterator():
            result[txnId] = self.nodeSerializer.deserialize(reply)['leaf_data']
        return result
