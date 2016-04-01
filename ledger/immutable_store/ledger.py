import datetime
import logging
import time
from collections import namedtuple

import plyvel

from ledger.immutable_store.error import GeneralMissingError
from ledger.immutable_store.merkle import TreeHasher
from ledger.immutable_store.merkle_tree import MerkleTree
from ledger.immutable_store.store import ImmutableStore, F

Reply = namedtuple('Reply', ['viewNo', 'reqId', 'result'])


class Ledger(ImmutableStore):
    def __init__(self, tree: MerkleTree, dir: str):
        """
        :param tree: an implementation of MerkleTree used to store events
        """
        # TODO The initialization logic should take care of migrating the
        # persisted data into a newly created Merkle Tree after server restart.
        self.dir = dir
        self.tree = tree
        self.hasher = TreeHasher()
        self._db = None
        self._reply = None
        self._processedReq = None
        self.start()
        self.serialNo = self.lastCount()
        self.recoverTree()

    def recoverTree(self):
        for key, entry in self._reply.iterator():
            record = eval(entry.decode('utf-8'))
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
            leaf_hash = self.hasher.hash_leaf(bytes(str(leaf_data), 'utf-8'))
            self.tree.append(leaf_hash)
            leaf_data_hash = leaf_hash
        else:
            raise GeneralMissingError("Transaction not found.")

    def _addToStore(self, data):
        serialNo = data['serial_no']
        key = bytes(str(serialNo).encode('utf-8'))
        self._reply.put(key, bytes(str(data), 'utf-8'))

    async def append(self, clientId: str, reply, txnId: str):
        txn = {
            "clientId": clientId,
            "reply": self._createReplyRecord(reply),
            "txnId": txnId
        }
        data = {
            'client_id': txn['clientId'],
            'request_id': txn['reply']['reqId'],
            'STH': self._getSTH(),
            'leaf_data': txn,
            'leaf_data_hash': self.hasher.hash_leaf(bytes(str(txn), 'utf-8')),
            'created': time.time(),
            'added_to_tree': time.time(),
            'audit_info': None
        }

        self.add(data)
        self.insertProcessedReq(clientId, reply.reqId, self.serialNo)

    async def get(self, clientId: str, reqId: int):
        serialNo = self.getProcessedReq(clientId, reqId)
        if serialNo:
            jsonReply = self._get(serialNo)[F.leaf_data.name]['reply']
            return self._createReplyFromJson(jsonReply)
        else:
            return serialNo

    def _get(self, serialNo):
        key = str(serialNo).encode('utf-8')
        value = self._reply.get(key)
        if value:
            return eval(value.decode('utf-8'))
        else:
            return value

    def insertProcessedReq(self, clientId, reqId, serial_no):
        key = bytes((clientId + "-" + str(reqId)), 'utf-8')
        value = bytes(str(serial_no), 'utf-8')
        self._processedReq.put(key, value)

    def getProcessedReq(self, clientId, reqId):
        key = bytes((clientId + "-" + str(reqId)), 'utf-8')
        serialNo = self._processedReq.get(key)
        if serialNo:
            return serialNo.decode('utf-8')
        else:
            return serialNo

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
        for key, _ in self._reply.iterator(reverse=True):
            return int(key.decode('utf-8'))
        return 0

    def _getSTH(self):
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
            logging.info("Ledger already started.")
        else:
            logging.info("Starting ledger...")
            self._db = plyvel.DB(self.dir, create_if_missing=True)
            self._reply = self._db.prefixed_db(b'reply')
            self._processedReq = self._db.prefixed_db(b'processedReq')

    def stop(self):
        self._db.close()
