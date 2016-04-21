import base64
from binascii import hexlify, unhexlify
from collections import namedtuple

import pytest
import time

from ledger.immutable_store.merkle import CompactMerkleTree, TreeHasher, \
    MerkleVerifier, FullMerkleTree


STH = namedtuple("STH", ["tree_size", "sha256_root_hash"])


@pytest.fixture()
def hasher():
    return TreeHasher()


@pytest.fixture()
def hasherAndFullMerkleTree(hasher):
    m = FullMerkleTree(hasher=hasher)
    return hasher, m


@pytest.fixture()
def hasherAndCompactMerkleTree(hasher):
    m = CompactMerkleTree(hasher=hasher)
    return hasher, m


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


def testFullMerkleTree(hasherAndFullMerkleTree):
    h, m = hasherAndFullMerkleTree
    verifier = MerkleVerifier()
    testStart = time.perf_counter()
    for d in range(100000):
        data = str(d+1).encode()
        m.append(data)
        if d % 1000 == 0:
            show(h, m, data)
            start = time.perf_counter()
            inc_proof = m.inclusion_proof(d, d + 1)

            print("Time taken to generate leaf inclusion proof in tree of size {} "
                  "is {}".format(d + 1, time.perf_counter() - start))
            sth = STH(d+1, m.root_hash())

            print("STH : {}".format(sth))
            print("audit_path : {}".format(inc_proof))
            print("size of audit_path : {}".format(len(inc_proof)))

            merkleTreeHead = base64.b64encode(m._calc_mth(0, d+1))
            if d > 7:
                print(base64.b64encode(m._calc_mth(0, d-6)).decode())
            print("merkle_tree_head : {}".format(merkleTreeHead))

            rootHash = base64.b64encode(m.root_hash())
            print("root_hash : {}".format(rootHash == merkleTreeHead))
            assert merkleTreeHead == rootHash

            start = time.perf_counter()
            assert verifier.verify_leaf_inclusion(data, d, [base64.b64decode(
                a.encode()) for a in inc_proof], sth)
            print("Time taken to verify leaf inclusion in tree of size {} from "
                  "audit path is {}".format(d+1, time.perf_counter()-start))

            if d in range(17001, 17002, 17003):
                inc_proof = m.inclusion_proof(d, d + 1)
                print("audit_path : {}".format(inc_proof))
                print("size of audit_path : {}".format(len(inc_proof)))
    print("Test ends in {}".format(time.perf_counter() - testStart))


def testCompactMerkleTree(hasherAndCompactMerkleTree):
    h, m = hasherAndCompactMerkleTree
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
