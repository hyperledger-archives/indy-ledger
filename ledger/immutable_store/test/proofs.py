# TODO Should this file be removed altogether?

import functools
import hashlib

from binascii import hexlify, unhexlify
from pprint import pprint

from ledger.immutable_store.merkle import MerkleVerifier, TreeHasher, \
    CompactMerkleTree
from ledger.immutable_store.stores.hash_store import HashStore
from ledger.immutable_store.stores.memory_hash_store import MemoryHashStore
from ledger.immutable_store.test.helper import checkLeafInclusion, \
    checkConsistency
from ledger.immutable_store.test.test_merkle_proof import STH


"""
TODO
    extracting

    CompactMerkleTree
    HashStore
    ConsistencyProof
    InclusionProof
    MTH (but modified with using the Node list from Hash Store
    Fast _path and _sub_proof from VL

    Pick an encoding and stick with it (binary, hexlified, base64, etc.)
"""


class NewMerkleTree:
    def __init__(self, hashStore: HashStore):
        self.hashStore = hashStore
        self.size = 0
        self.hasher = TreeHasher()

    def appendHash(self, h):
        self.hashStore.writeLeaf(h)
        self.size += 1

    def appendData(self, data):
        leafHash = self.hasher.hash_leaf(data)
        self.appendHash(leafHash)

    # def get_entries(self, start, end):
    #     return self._entries[start:end + 1]
    #
    def get_tree_head(self, seq=None):
        if seq is None:
            seq = self.size
        if seq > self.size:
            raise IndexError
        return {
            'tree_size': seq,
            'sha256_root_hash': self._calc_mth(0, seq) if seq else None,
        }

    def consistency_proof(self, first, second):
        # return [base64.b64encode(self._calc_mth(a, b)) for a, b in
        #         self._subproof(first, 0, second, True)]
        proof = []
        for a, b in self._subproof(first, 0, second, True):
            print("in cons proof {} {}".format(a, b))
            mth = self._calc_mth(a, b)
            proof.append(mth)
        return proof

    def inclusion_proof(self, m, n):
        # return [base64.b64encode(self._calc_mth(a, b)) for a, b in
        #         self._path(m, 0, n)]
        proof = []
        for a, b in self._path(m, 0, n):
            mth = hexlify(self._calc_mth(a, b))
            proof.append(mth)
        return proof

    @functools.lru_cache(maxsize=256)
    def _calc_mth(self, start, end):
        # for calculating mth, successively hash the leaf with hashes in its
        # audit path
        stack = []
        tree_size = end - start
        # TODO INEFFICIENT!!! change to use stored non-leaf-node hashes
        for idx, leaf in enumerate(self.hashStore.readLeafs(start+1, end+1)):
            stack.append(leaf)
            if idx + 1 < tree_size:
                # lowest bit which is not set
                rng = bin(idx).replace('b', '')[::-1].index('0')
            else:
                rng = len(stack) - 1
            for _ in range(rng):
                stack[-2:] = [
                    hashlib.sha256(chr(1).encode() + stack[-2] + stack[-1]).digest()]

        path = self.hashStore.getPath(end, start)
        leafHash = self.hashStore.readLeaf(end)
        hashes = [leafHash, ]
        if path[0]:
            hashes.append(self.hashStore.readLeaf(path[0][0]))
        # for h in path[1]:
        #     hashes.append(self.hashStore.readNode(h))
        # foldedHash = self.hasher._hash_fold(hashes)
        # print("leaf hash: {}".format(leafHash))
        # print("_calc_mth hash: {}".format(hexlify(stack[0])))
        # print("hashstore path: {}".format(path))
        # print("folded hash: {}".format(foldedHash))
        # print("hash from hashstore path: {}".
        # format(self.hashStore._calcHash(end, start)))

        return stack[0]

    def _subproof(self, m, start_n, end_n, b):
        n = end_n - start_n
        if m == n:
            if b:
                return []
            else:
                return [(start_n, end_n)]
        else:
            k = 1 << (len(bin(n - 1)) - 3)
            if m <= k:
                return self._subproof(m, start_n, start_n + k, b) + [
                    (start_n + k, end_n)]
            else:
                return self._subproof(m - k, start_n + k, end_n, False) + [
                    (start_n, start_n + k)]

    def _path(self, m, start_n, end_n):
        n = end_n - start_n
        if n == 1:
            return []
        else:
            # `k` is the largest power of 2 less than `n`
            k = 1 << (len(bin(n - 1)) - 3)
            if m < k:
                return self._path(m, start_n, start_n + k) + [
                    (start_n + k, end_n)]
            else:
                return self._path(m - k, start_n + k, end_n) + [
                    (start_n, start_n + k)]


def testVerifiableLog():
    print()
    vl = NewMerkleTree(MemoryHashStore())
    v = MerkleVerifier()

    showdata = {}
    txncnt = 1000
    for seq in range(1, txncnt+1):
        data = str(seq).encode()
        vl.appendData(data)
        incl_proof = vl.inclusion_proof(seq-1, seq)
        tree_head = vl.get_tree_head()
        root_hash = hexlify(tree_head['sha256_root_hash'])
        showdata["a-seq"] = seq
        showdata["b-root hash"] = root_hash
        showdata["c-incl proof"] = incl_proof
        incl_proof_bin = [unhexlify(p) for p in incl_proof]

        # pprint(showdata, width=120)

        checkLeafInclusion(v, leafData=data, leafIndex=seq-1,
                           proof=incl_proof_bin, treeHead=tree_head)

    checkConsistency(vl, verifier=v)


def testHashStoreGetPath():
    print()
    pprint(HashStore.getPath(8))
    pprint(HashStore.getPath(8, 4))
