import base64
from binascii import hexlify, unhexlify
from collections import namedtuple
from copy import copy

import pytest
import time

from ledger.immutable_store.merkle import CompactMerkleTree, TreeHasher, \
    MerkleVerifier, FullMerkleTree, count_bits_set, highest_bit_set, HashStore

STH = namedtuple("STH", ["tree_size", "sha256_root_hash"])


class MemoryHashStore(HashStore):
    def __init__(self):
        self.nodes = []
        self.leafs = []

    def writeLeaf(self, leaf):
        self.leafs.append(leaf)

    def writeNode(self, node):
        self.nodes.append(node)

    def getLeaf(self, pos):
        return self.leafs[pos-1]

    def getNode(self, pos):
        return self.nodes[pos-1]


@pytest.fixture()
def hasher():
    return TreeHasher()


# @pytest.fixture()
# def hasherAndFullMerkleTree(hasher):
#     m = FullMerkleTree(hasher=hasher)
#     return hasher, m


@pytest.fixture()
def hasherAndTree(hasher):
    m = CompactMerkleTree(hasher=hasher)
    return hasher, m


@pytest.fixture()
def addTxns(hasherAndTree):
    h, m = hasherAndTree

    txn_count = 1000

    auditPaths = []
    for d in range(txn_count):
        serNo = d+1
        data = str(serNo).encode()
        auditPaths.append([hexlify(h) for h in m.append(data)])

    return txn_count, auditPaths


def getNodePosition(start, height=None):
    pwr = highest_bit_set(start) - 1
    height = height or pwr
    if count_bits_set(start) == 1:
        adj = height - pwr
        return start - 1 + adj
    else:
        c = pow(2, pwr)
        return getNodePosition(c, pwr) + getNodePosition(start - c, height)


def show(h, m, data):
    print("-" * 60)
    print("appended  : {}".format(data))
    print("hash      : {}".format(hexlify(h.hash_leaf(data))[:3]))
    print("tree size : {}".format(m.tree_size))
    print("root hash : {}".format(m.root_hash_hex()[:3]))
    for i, hash in enumerate(m.hashes):
        lead = "Hashes" if i == 0 else "      "
        print("{}    : {}".format(lead, hexlify(hash)[:3]))


"""
1: 221

  [221]
  /
221

2: e8b

  [e8b]
  /   \
221   fa6

3: e8b, 906

      fe6
     /   \
  [e8b]  [906]
  /   \
221   fa6


4: 4c4

        [4c4]
     /         \
   e8b         9c7
  /   \       /   \
221   fa6   906   11e


5: 4c4, 533

               e10
            /       \
        [4c4]       [533]
     /         \
   e8b         9c7
  /   \       /   \
221   fa6   906   11e


6: 4c4, 2b1

                 ecc
            /          \
        [4c4]          [2b1]
     /         \       /   \
   e8b         9c7   533   3bf
  /   \       /   \
221   fa6   906   11e


7: 4c4, 2b1, 797

                    74f
            /                \
        [4c4]                429
     /         \            /    \
   e8b         9c7       [2b1]  [797]
  /   \       /   \      /   \
221   fa6   906   11e  533   3bf


8: 50f
                    [50f]
            /                \
         4c4                 fed
     /         \            /    \
   e8b         9c7        2b1     800
  /   \       /   \      /   \    /  \
221   fa6   906   11e  533   3bf 797 754


"""

"""
hexlify(c(
    c(
        c(
            l(d[0]), l(d[1])
        ),
        c(
            l(d[2]), l(d[3])
        )
    ),
    c(
        c(
            l(d[4]),l(d[5])
        ),
        l(d[6])
    )
))
"""


# def testFullMerkleTree(hasherAndFullMerkleTree):
#     h, m = hasherAndFullMerkleTree
#     verifier = MerkleVerifier()
#     testStart = time.perf_counter()
#     for d in range(100000):
#         data = str(d+1).encode()
#         m.append(data)
#         if d % 1000 == 0:
#             show(h, m, data)
#             start = time.perf_counter()
#             inc_proof = m.inclusion_proof(d, d + 1)
#
#             print("Time taken to generate leaf inclusion proof in tree of size {} "
#                   "is {}".format(d + 1, time.perf_counter() - start))
#             sth = STH(d+1, m.root_hash())
#
#             print("STH : {}".format(sth))
#             print("audit_path : {}".format(inc_proof))
#             print("size of audit_path : {}".format(len(inc_proof)))
#
#             merkleTreeHead = base64.b64encode(m._calc_mth(0, d+1))
#             if d > 7:
#                 print(base64.b64encode(m._calc_mth(0, d-6)).decode())
#             print("merkle_tree_head : {}".format(merkleTreeHead))
#
#             rootHash = base64.b64encode(m.root_hash())
#             print("root_hash : {}".format(rootHash == merkleTreeHead))
#             assert merkleTreeHead == rootHash
#
#             start = time.perf_counter()
#             assert verifier.verify_leaf_inclusion(data, d, [base64.b64decode(
#                 a.encode()) for a in inc_proof], sth)
#             print("Time taken to verify leaf inclusion in tree of size {} from "
#                   "audit path is {}".format(d+1, time.perf_counter()-start))
#
#             if d in range(17001, 17002, 17003):
#                 inc_proof = m.inclusion_proof(d, d + 1)
#                 print("audit_path : {}".format(inc_proof))
#                 print("size of audit_path : {}".format(len(inc_proof)))
#     print("Test ends in {}".format(time.perf_counter() - testStart))


def testCompactMerkleTree(hasherAndTree):
    h, m = hasherAndTree
    verifier = MerkleVerifier()
    for d in range(100000):
        data = str(d + 1).encode()
        auditPath = m.append(data)
        if d % 1000 == 0:
            show(h, m, data)
            print("audit path is {}".format([hexlify(ap) for ap in auditPath]))
            print("audit path length is {}".format(verifier.audit_path_length(d, d+1)))
            print("audit path calculated length is {}".format(
                len(auditPath)))
            calculated_root_hash = verifier._calculate_root_hash_from_audit_path(
                h.hash_leaf(data), d, auditPath[:], d+1)
            print("calculated root hash is {}".format(hexlify(calculated_root_hash)))
            sth = STH(d+1, m.root_hash())
            verifier.verify_leaf_inclusion(data, d, auditPath, sth)


@pytest.fixture()
def storeHashes(hasherAndTree, addTxns):
    h, m = hasherAndTree

    mhs = MemoryHashStore()

    while m.leaf_hash_deque:
        mhs.writeLeaf(m.leaf_hash_deque.pop())

    while m.node_hash_deque:
        mhs.writeNode(m.node_hash_deque.pop())

    return mhs


def testEfficientHashStore(hasherAndTree, addTxns, storeHashes):
    h, m = hasherAndTree

    mhs = storeHashes
    txnCount, auditPaths = addTxns
    assert len(mhs.leafs) == txnCount

    for leaf in mhs.leafs:
        print("leaf hash: {}".format(hexlify(leaf)))

    node_ptr = 0
    for n in mhs.nodes:
        start, height, node_hash = n
        node_ptr += 1
        end = start - pow(2, height) + 1
        print("node hash start-end: {}-{}".format(start, end))
        print("node hash height: {}".format(height))
        print("node hash end: {}".format(end))
        print("node hash: {}".format(hexlify(node_hash)))
        assert getNodePosition(start, height) == node_ptr

'''
14
pwr = 3
c = 8

14,8
pwr = 2
c = 4 + 8 = 12

12,2

14, 12
pwr = 1
c = 2 + 12 = 14

14,1
'''


def getPath(serNo, offset=0):
    pwr = highest_bit_set(serNo-1-offset) - 1
    if pwr <= 0:
        if serNo % 2 == 0:
            return [serNo - 1], []
        else:
            return [], []
    c = pow(2, pwr) + offset
    leafs, nodes = getPath(serNo, c)
    nodes.append(getNodePosition(c, pwr))
    return leafs, nodes


def testLocate(hasherAndTree, addTxns, storeHashes):
    h, m = hasherAndTree

    mhs = storeHashes
    txnCount, auditPaths = addTxns

    verifier = MerkleVerifier()

    for d in range(50):
        print()
        pos = d+1
        print("Audit Path for Serial No: {}".format(pos))
        leafs, nodes = getPath(pos)
        calculatedAuditPath = []
        for i, leaf_pos in enumerate(leafs):
            hash = hexlify(mhs.getLeaf(leaf_pos))
            print("leaf: {}".format(hash))
            calculatedAuditPath.append(hash)
        for node_pos in nodes:
            start, height, node = mhs.getNode(node_pos)
            hash = hexlify(node)
            print("node: {}".format(hash))
            calculatedAuditPath.append(hash)
        print("{} -> leafs: {}, nodes: {}".format(pos, leafs, nodes))
        print("Audit path built using formula {}".format(calculatedAuditPath))
        print("Audit path received while appending leaf {}".format(auditPaths[d]))

        # Testing equality of audit path calculated using formula and audit path
        #  received while inserting leaf into the tree
        assert calculatedAuditPath == auditPaths[d]
        auditPathLength = verifier.audit_path_length(d, d+1)
        assert auditPathLength == len(calculatedAuditPath)

        # Testing root hash generation
        leafHash = storeHashes.getLeaf(d)
        rootHashFrmCalc = verifier._calculate_root_hash_from_audit_path(
            leafHash, d, copy(calculatedAuditPath), d+1)
        rootHash = verifier._calculate_root_hash_from_audit_path(
            leafHash, d, copy(auditPaths[d]), d + 1)
        assert rootHash == rootHashFrmCalc

        # Testing verification, do not need `assert` since
        # `verify_leaf_hash_inclusion` will throw an exeception
        sthFrmCalc = STH(d + 1, rootHashFrmCalc)
        verifier.verify_leaf_hash_inclusion(leafHash, d, calculatedAuditPath, sthFrmCalc)
        sth = STH(d + 1, rootHash)
        verifier.verify_leaf_hash_inclusion(leafHash, d, auditPaths[d], sth)