import logging
import time
from collections import namedtuple
import pickle

import plyvel

from ledger.immutable_store.error import GeneralMissingError
from ledger.immutable_store.file_store import FileStore
from ledger.immutable_store.merkle import TreeHasher
from ledger.immutable_store.merkle_tree import MerkleTree
from ledger.immutable_store.store import ImmutableStore, F

Reply = namedtuple('Reply', ['viewNo', 'reqId', 'result'])


class Ledger(ImmutableStore):
    def __init__(self, tree: MerkleTree, dataDir: str):
        """
        :param tree: an implementation of MerkleTree used to store events
        """
        # TODO The initialization logic should take care of migrating the
        # persisted data into a newly created Merkle Tree after server restart.
        self.dataDir = dataDir
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
            record = pickle.loads(entry)
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
        self._reply.put(key, pickle.dumps(data))

    async def append(self, identifier: str, reply, txnId: str):
        txn = {
            "identifier": identifier,
            "reply": self._createReplyRecord(reply),
            "txnId": txnId
        }
        data = {
            'identifier': txn['identifier'],
            'request_id': 1,
            'STH': 1,
            'leaf_data': txn,
            'leaf_data_hash': self.hasher.hash_leaf(bytes(str(txn), 'utf-8')),
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
        key = str(serialNo).encode('utf-8')
        value = self._reply.get(key)
        if value:
            return pickle.loads(value)
        else:
            return value

    def insertProcessedReq(self, identifier, reqId, serial_no):
        key = bytes((identifier + "-" + str(reqId)), 'utf-8')
        value = bytes(str(serial_no), 'utf-8')
        self._processedReq.put(key, value)

    def getProcessedReq(self, identifier, reqId):
        key = bytes((identifier + "-" + str(reqId)), 'utf-8')
        serialNo = self._processedReq.get(key)
        if serialNo:
            return serialNo.decode('utf-8')
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
        return 0 if key is None else int(int(key.decode('utf-8')))

    def size(self):
        return self.serialNo

    def start(self, loop=None):
        if self._reply or self._processedReq:
            logging.info("Ledger already started.")
        else:
            logging.info("Starting ledger...")
            # self._db = plyvel.DB(self.dir, create_if_missing=True)
            # self._reply = self._db.prefixed_db(b'reply')
            # self._processedReq = self._db.prefixed_db(b'processedReq')
            self._reply = FileStore(self.dataDir, "reply")
            self._processedReq = FileStore(self.dataDir, "processedReq")

    def stop(self):
        self._reply.close()
        self._processedReq.close()

    def reset(self):
        self._reply.reset()
        self._processedReq.reset()

    def getAllTxn(self):
        result = {}
        for txnId, reply in self._reply.iterator():
            result[txnId.decode()] = pickle.loads(reply)['leaf_data']['reply'][
                'result']
        return result
