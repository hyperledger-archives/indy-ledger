# Basic verifiable log, for demo purposes only
from base64 import b64encode

from verifiable_map import hashleaf
from verifiable_map import hashparent


class VerifiableLog:
    def __init__(self):
        self._entries = []
        self._cache = {}

    def append(self, val):
        self._entries.append(val)

    def get_entries(self, start, end):
        return self._entries[start:end + 1]

    def get_tree_head(self, seq=None):
        if seq is None:
            seq = len(self._entries)
        assert seq <= len(self._entries)
        return {
            'tree_size': seq,
            'sha256_root_hash': b64encode(
                self._calc_mth(0, seq)) if seq else None,
        }

    def consistency_proof(self, first, second):
        return [b64encode(self._calc_mth(a, b)) for a, b in
                self._subproof(first, 0, second, True)]

    def inclusion_proof(self, m, n):
        return [b64encode(self._calc_mth(a, b)) for a, b in
                self._path(m, 0, n)]

    def _calc_mth(self, start, end):
        k = '%i-%i' % (start, end)
        rv = self._cache.get(k, None)
        if not rv:
            stack = []
            tree_size = end - start
            for idx, leaf in enumerate(self._entries[start:end]):
                stack.append(hashleaf(leaf))
                for _ in range(bin(idx).replace('b', '')[::-1].index(
                        '0') if idx + 1 < tree_size else len(stack) - 1):
                    stack[-2:] = [hashparent(stack[-2], stack[-1])]
            rv = stack[0]
            self._cache[k] = rv
        return rv

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
            k = 1 << (len(bin(n - 1)) - 3)
            if m < k:
                return self._path(m, start_n, start_n + k) + [
                    (start_n + k, end_n)]
            else:
                return self._path(m - k, start_n + k, end_n) + [
                    (start_n, start_n + k)]
