import base64
from collections import OrderedDict

import time


from ledger.immutable_store.ledger import Ledger
from ledger.immutable_store.merkle import CompactMerkleTree, TreeHasher

# TODO Remove hard-coded CompactMerkleTree
from ledger.immutable_store.serializers import JsonSerializer
from ledger.immutable_store.serializers.compact_serializer import \
    CompactSerializer

levelDBDir = "/tmp/testLedger"
hasher = TreeHasher()


def b64e(s):
    return '"{}"'.format(base64.b64encode(s).decode("utf-8"))


def b64d(s):
    return base64.b64decode(s)


orderedFields = OrderedDict([
    ("serial_no", (str, int)),
    ("STH.tree_size", (str, int)),
    ("STH.root_hash", (b64e, b64d)),
    ("leaf_data.identifier", (str, str)),
    ("leaf_data.reqId", (str, int)),
    ("leaf_data.op", (str, str)),
    ("leaf_data.reference", (str, str)),
    ("leaf_data_hash", (b64e, b64d)),
    ("audit_info", (str, list))
])

ledgerSerializer = CompactSerializer(orderedFields)
leafSerializer = JsonSerializer()


def testAddTxn():

    ledger = Ledger(CompactMerkleTree(), dataDir=levelDBDir, serializer=ledgerSerializer)
    txn1 = {
        'identifier': 'cli1',
        'reqId': 1,
        'op': 'do something',
        'reference': 'K2GI8SX89'
    }
    data_to_persist1 = {
        "serial_no": 0,
        'STH': ledger.getSTH(),
        'leaf_data': txn1,
        'leaf_data_hash': hasher.hash_leaf(leafSerializer.serialize(
            txn1)),
        'audit_info': ["e3b0c44298fc1c", "fbf4c8996fb92427", "a495991b78"],
    }

    ledger.add(data_to_persist1)

    txn2 = {
        'identifier': 'cli1',
        'reqId': 2,
        'op': 'do something else',
        'reference': 'K2GI8SX81'
    }
    data_to_persist2 = {
        "serial_no": 1,
        'STH': ledger.getSTH(),
        'leaf_data': txn2,
        'leaf_data_hash': hasher.hash_leaf(leafSerializer.serialize(
            txn1)),
        'audit_info': ["e3b0c44298fc1d", "fbf4c8996fb92428", "a495991b79"],
    }

    ledger.add(data_to_persist2)

    # Check that the transaction is added to the Merkle Tree
    assert ledger.size == 2

    # Check that the data is appended to the immutable store
    assert data_to_persist1['leaf_data'] == ledger.getBySerialNo(0)['leaf_data']
    assert data_to_persist2['leaf_data'] == ledger.getBySerialNo(1)['leaf_data']
    ledger.reset()

"""
If the server holding the ledger restarts, the ledger should be fully rebuilt
from persisted data. Any incoming commands should be stashed. (Does this affect
creation of Signed Tree Heads? I think I don't really understand what STHs are.)
"""


def testRecoverMerkleTreeFromLedger():
    ledger2 = Ledger(CompactMerkleTree(), dataDir=levelDBDir, serializer=ledgerSerializer)
    assert ledger2.tree.root_hash() is not None
    ledger2.reset()
    ledger2.stop()


# def testTearDown():
#     ledger_db.drop()

