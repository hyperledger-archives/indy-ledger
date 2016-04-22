from binascii import hexlify
from pprint import pprint
from string import ascii_lowercase

import pytest
from math import log

from ledger.immutable_store.merkle import CompactMerkleTree, TreeHasher, \
    MerkleVerifier, count_bits_set, highest_bit_set, HashStore


@pytest.fixture()
def hasherAndTree():
    h = TreeHasher()
    m = CompactMerkleTree(hasher=h)

    def show(data):
        print("-" * 60)
        print("appended  : {}".format(data))
        print("hash      : {}".format(hexlify(h.hash_leaf(data))[:3]))
        print("tree size : {}".format(m.tree_size))
        print("root hash : {}".format(m.root_hash_hex()[:3]))
        for i, hash in enumerate(m.hashes):
            lead = "Hashes" if i == 0 else "      "
            print("{}    : {}".format(lead, hexlify(hash)[:3]))

    return h, m, show

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


def testStuff(hasherAndTree):
    h, m, show = hasherAndTree
    verifier = MerkleVerifier()

    for d in range(10000):
        data = str(d+1).encode()
        m.append(data)
        audit_path_length = verifier.audit_path_length(d, d+1)
        incl_proof = m.inclusion_proof(d, d+1)  # size before and size after
        if d % 100 == 0:
            show(data)
            print("aud path l: {}".format(audit_path_length))
            print("incl proof: {}".format(incl_proof))
            print("incl p len: {}".format(len(incl_proof)))


    # m.append(b'a')
    #
    #
    #
    # print("")
    # for d in range(0, 100):
    #     m.append(str(d).encode())
    #     print("s:{} h:{}".format(m.tree_size, len(m.hashes)))
    # m.append(b'hello')
    #
    # class O:
    #     def __init__(self):
    #         self.tree_size = None
    #         self.hashes = []
    #
    # o = O()
    # m.save(o)
    # print(o)


def getNodePosition(start, height=None):
    pwr = highest_bit_set(start) - 1
    height = height or pwr
    if count_bits_set(start) == 1:
        adj = height - pwr
        return start - 1 + adj
    else:
        c = pow(2, pwr)
        return getNodePosition(c, pwr) + getNodePosition(start - c, height)


@pytest.fixture()
def addTxns(hasherAndTree):
    h, m, show = hasherAndTree

    txn_count = 1000

    auditPaths = []
    for d in range(txn_count):
        serNo = d+1
        data = str(serNo).encode()
        auditPaths.append([hexlify(h) for h in m.append(data)])

    return txn_count, auditPaths


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
def storeHashes(hasherAndTree, addTxns):
    h, m, show = hasherAndTree

    mhs = MemoryHashStore()

    while m.leaf_hash_deque:
        mhs.writeLeaf(m.leaf_hash_deque.pop())

    while m.node_hash_deque:
        mhs.writeNode(m.node_hash_deque.pop())

    return mhs


def testEfficientHashStore(hasherAndTree, addTxns, storeHashes):
    h, m, show = hasherAndTree

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
            return [serNo-1], []
        else:
            return [], []
    c = pow(2, pwr) + offset
    leafs, nodes = getPath(serNo, c)
    nodes.append(getNodePosition(c, pwr))
    return leafs, nodes


def testLocate(hasherAndTree, addTxns, storeHashes):
    h, m, show = hasherAndTree

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
        print("Audit path built using formula {}".format(calculatedAuditPath))
        print("Audit path received while appending leaf {}".format(auditPaths[d]))
        assert calculatedAuditPath == auditPaths[d]

        print("{} -> leafs: {}, nodes: {}".format(pos, leafs, nodes))

