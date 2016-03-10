from immutable_store.ledger import Ledger
from immutable_store.merkle import CompactMerkleTree, TreeHasher
from immutable_store.mongodb_ledger import MongoDBLedger
import time

# TODO Remove hard-coded CompactMerkleTree
ledger_db = MongoDBLedger('testLedger', 'ledger')
ledger = Ledger(CompactMerkleTree(), ledger_db)

hasher = TreeHasher()


def testAddTxn():
    txn = {
        'clientId': 'cli1',
        'reqId': 1,
        'op': 'do something',
        'reference': 'K2GI8SX89'
    }
    data_to_persist = {
        'client_id': txn['clientId'],
        'request_id': 1,
        'STH': 1,
        'leaf_data': txn,
        'leaf_data_hash': hasher.hash_leaf(bytes(str(txn), 'utf-8')),
        'created': time.time(),
        'added_to_tree': time.time(),
        'audit_info': None,
        'seq_no': 1
    }

    ledger.addTxn(data_to_persist)

    # Check that the transaction is added to the Merkle Tree
    assert len(ledger.tree)
    # leaf_data_hash = data_to_persist['leaf_data_hash']
    # assert ledger.tree.root_hash() == \
    #        hasher.hash_children(leaf_data_hash, leaf_data_hash)

    # Check that the data is appended to the immutable store
    assert data_to_persist['leaf_data'] == ledger.store.getTxnBySeqNo(1)['leaf_data']

"""
If the server holding the ledger restarts, the ledger should be fully rebuilt
from persisted data. Any incoming commands should be stashed. (Does this affect
creation of Signed Tree Heads? I think I don't really understand what STHs are.)
"""


def testRecoverMerkleTreeFromLedger():
    ledger2 = Ledger(CompactMerkleTree(), ledger_db)
    assert ledger.tree.root_hash() == ledger2.tree.root_hash()


def testTearDown():
    ledger_db._ledger.drop()
