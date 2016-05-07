import base64
from collections import OrderedDict
from tempfile import TemporaryDirectory

import pytest
from ledger.ledger import Ledger
from ledger.serializers.json_serializer import JsonSerializer
from ledger.serializers.compact_serializer import CompactSerializer
from ledger.compact_merkle_tree import CompactMerkleTree


def b64e(s):
    return base64.b64encode(s).decode("utf-8")


def b64d(s):
    return base64.b64decode(s)


def lst2str(l):
    return ",".join(l)


orderedFields = OrderedDict([
    ("identifier", (str, str)),
    ("reqId", (str, int)),
    ("op", (str, str))
])

ledgerSerializer = CompactSerializer(orderedFields)
leafSerializer = JsonSerializer()


@pytest.yield_fixture(scope='module')
def tempdir():
    with TemporaryDirectory() as tdir:
        yield tdir


# @pytest.mark.skipif(True, reason="Obsolete")
def testAddTxn(tempdir):
    ledger = Ledger(CompactMerkleTree(), dataDir=tempdir,
                    serializer=ledgerSerializer)
    ledger.reset()

    txn1 = {
        'identifier': 'cli1',
        'reqId': 1,
        'op': 'do something'
    }
    ledger.add(txn1)

    txn2 = {
        'identifier': 'cli1',
        'reqId': 2,
        'op': 'do something else'
    }
    ledger.add(txn2)

    # Check that the transaction is added to the Merkle Tree
    assert ledger.size == 2

    # Check that the data is appended to the immutable store
    assert txn1 == ledger.getBySeqNo(1)
    assert txn2 == ledger.getBySeqNo(2)


"""
If the server holding the ledger restarts, the ledger should be fully rebuilt
from persisted data. Any incoming commands should be stashed. (Does this affect
creation of Signed Tree Heads? I think I don't really understand what STHs are.)
"""


def testRecoverMerkleTreeFromLedger(tempdir):
    ledger2 = Ledger(CompactMerkleTree(), dataDir=tempdir,
                     serializer=ledgerSerializer)
    assert ledger2.tree.root_hash() is not None
    ledger2.reset()
    ledger2.stop()
