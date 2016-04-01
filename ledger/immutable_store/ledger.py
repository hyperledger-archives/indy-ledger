import datetime
import logging
import time
from collections import namedtuple
import json

from ledger.immutable_store.base64_serializer import Base64Serializer
from ledger.immutable_store.error import GeneralMissingError
from ledger.immutable_store.mappingserializer import MappingSerializer
from ledger.immutable_store.merkle import TreeHasher
from ledger.immutable_store.merkle_tree import MerkleTree
from ledger.immutable_store.store import ImmutableStore, F
from ledger.immutable_store.text_file_store import TextFileStore


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
        self.serializer = serializer or Base64Serializer() # type: MappingSerializer
        self.hasher = TreeHasher()
        self._db = None
        self._reply = None
        self._processedReq = None
        self.start()
        self.serialNo = self.lastCount()
        self.recoverTree()

    def recoverTree(self):
        for key, entry in self._reply.iterator():
            record = self.serializer.deserialize(entry)
            self._addToTree(record)

    def add(self, data):
        self.serialNo += 1
        data['serial_no'] = self.serialNo
        self._addToTree(data)
        self._addToStore(data)

    def _addToTree(self, data):
        leaf_data_hash = data[F.leaf_data_hash.name]
        leaf_data = data[F.leaf_data.name]
        if leaf_data_hash:
            if isinstance(leaf_data_hash, str):
                leaf_data_hash = leaf_data_hash.encode()
            self.tree.append(leaf_data_hash)
        elif leaf_data:
            leaf_hash = self.hasher.hash_leaf(self.serializer.serialize(
                leaf_data))
            self.tree.append(leaf_hash)
        else:
            raise GeneralMissingError("Transaction not found.")

    def _addToStore(self, data):
        serialNo = data['serial_no']
        key = str(serialNo)
        self._reply.put(key, self.serializer.serialize(
            data, toBytes=False))

    async def append(self, identifier: str, reply, txnId: str):
        # TODO: audit_info are missing Merkle tree is implementation
        # is incomplete
        data = {
            'STH': self.getSTH(),
            'leaf_data': reply.result,
            'leaf_data_hash': self.hasher.hash_leaf(self.serializer.serialize(
                reply.result)
            ),
            'audit_info': None      # TODO: Implement this
        }

        self.add(data)
        self.insertProcessedReq(identifier, reply.reqId, self.serialNo)

    async def get(self, identifier: str, reqId: int):
        serialNo = self.getProcessedReq(identifier, reqId)
        if serialNo:
            return self._get(serialNo)[F.leaf_data.name]
        else:
            return None

    def _get(self, serialNo):
        key = str(serialNo)
        value = self._reply.get(key)
        if value:
            return self.serializer.deserialize(value)
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
            "version": "0.0.1",
            "signature_type": "tree_hash",
            "timestamp": str(datetime.datetime.utcnow()),
            "tree_size": self.tree.tree_size + 1,
            "sha256_root_hash": self.tree.root_hash_hex()
        }

    def size(self):
        return self.serialNo

    def start(self, loop=None):
        if self._reply or self._processedReq:
            logging.debug("Ledger already started.")
        else:
            logging.debug("Starting ledger...")
            self._reply = TextFileStore(self.dataDir, "reply")
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
            result[txnId] = self.serializer.\
                deserialize(reply)['leaf_data']
        return result
