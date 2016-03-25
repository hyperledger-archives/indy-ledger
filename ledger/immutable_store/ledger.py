import logging
import time
from collections import namedtuple
import json

from ledger.immutable_store.base64_serializer import Base64Serializer
from ledger.immutable_store.error import GeneralMissingError
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
        self._leafDataFields = ["identifier", "request_id", "STH",
                                "leaf_data", "leaf_data_hash", "created",
                                "added_to_tree", "audit_info", "serial_no"]

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
            self.tree.append(leaf_data_hash)
        elif leaf_data:
            leaf_hash = self.hasher.hash_leaf(self.serializer.serialize(
                leaf_data, fields=self._leafDataFields))
            self.tree.append(leaf_hash)
        else:
            raise GeneralMissingError("Transaction not found.")

    def _addToStore(self, data):
        serialNo = data['serial_no']
        key = str(serialNo)
        self._reply.put(key, self.serializer.serialize(
            data, fields=self._leafDataFields, toBytes=False))

    async def append(self, identifier: str, reply, txnId: str):
        txn = {
            "identifier": identifier,
            "reply": self._createReplyRecord(reply),
            "txnId": txnId
        }
        # TODO: STH and audit_info are missing Merkle tree is implementation
        # is incomplete
        data = {
            'identifier': txn['identifier'],
            'request_id': reply.reqId,
            'STH': 1,
            'leaf_data': txn,
            'leaf_data_hash': self.hasher.hash_leaf(self.serializer.serialize(
                txn, fields=["identifier", "reply", "txnId"])
            ),
            'created': time.time(),
            'added_to_tree': time.time(),
            'audit_info': None
        }
        self.add(data)
        self.insertProcessedReq(identifier, reply.reqId, self.serialNo)

    async def get(self, identifier: str, reqId: int):
        serialNo = self.getProcessedReq(identifier, reqId)
        if serialNo:
            jsonReply = self._get(serialNo)[F.leaf_data.name]['reply']
            return self._createReplyFromJson(jsonReply)
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

    def _createReplyRecord(self, reply):
        return {
            "viewNo": reply.viewNo,
            "reqId": reply.reqId,
            "result": reply.result}

    def _createReplyFromJson(self, jsonReply):
        return Reply(jsonReply["viewNo"],
                     jsonReply["reqId"],
                     jsonReply["result"])

    def lastCount(self):
        key = self._reply.lastKey
        return 0 if key is None else int(key)

    def size(self):
        return self.serialNo

    def start(self, loop=None):
        if self._reply or self._processedReq:
            logging.info("Ledger already started.")
        else:
            logging.info("Starting ledger...")
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
                deserialize(reply)['leaf_data']['reply']['result']
        return result
