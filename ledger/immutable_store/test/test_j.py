from binascii import hexlify

import pytest

from ledger.immutable_store.merkle import CompactMerkleTree, TreeHasher, \
    MerkleVerifier


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
        audit_info = m.inclusion_proof(d, d+1)
        if d % 100 == 0:
            show(data)

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
