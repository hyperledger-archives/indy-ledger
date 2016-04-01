import time


from ledger.immutable_store.ledger import Ledger
from ledger.immutable_store.merkle import CompactMerkleTree, TreeHasher

# TODO Remove hard-coded CompactMerkleTree
levelDBDir = "/tmp/testLedger"
hasher = TreeHasher()


def testAddTxn():
    ledger = Ledger(CompactMerkleTree(), levelDBDir)
    txn1 = {
        'identifier': 'cli1',
        'reqId': 1,
        'op': 'do something',
        'reference': 'K2GI8SX89'
    }
    data_to_persist1 = {
        'STH': ledger.getSTH(),
        'leaf_data': txn1,
        'leaf_data_hash': hasher.hash_leaf(ledger.serializer.serialize(
            txn1)),
        'audit_info': None,
    }

    ledger.add(data_to_persist1)

    txn2 = {
        'identifier': 'cli1',
        'reqId': 2,
        'op': 'do something',
        'reference': 'K2GI8SX89'
    }
    data_to_persist2 = {
        'STH': ledger.getSTH(),
        'leaf_data': txn2,
        'leaf_data_hash': hasher.hash_leaf(ledger.serializer.serialize(
            txn1)),
        'audit_info': None
    }

    ledger.add(data_to_persist2)

    # Check that the transaction is added to the Merkle Tree
    assert len(ledger.tree) == 2

    # Check that the data is appended to the immutable store
    assert data_to_persist1['leaf_data'] == ledger._get(1)['leaf_data']
    ledger.reset()

"""
If the server holding the ledger restarts, the ledger should be fully rebuilt
from persisted data. Any incoming commands should be stashed. (Does this affect
creation of Signed Tree Heads? I think I don't really understand what STHs are.)
"""


def testRecoverMerkleTreeFromLedger():
    ledger2 = Ledger(CompactMerkleTree(), levelDBDir)
    assert ledger2.tree.root_hash() is not None
    ledger2.reset()
    ledger2.stop()


# def testTearDown():
#     ledger_db.drop()

