from ledger.immutable_store.stores.file_hash_store import FileHashStore


class TestFileHashStore(FileHashStore):
    @staticmethod
    def dataGen(clbk):
        i = 1
        while True:
            try:
                data = clbk(i)
            except ValueError:
                break
            yield data
            i += 1

    @property
    def leafs(self):
        return self.dataGen(self.getLeaf)

    @property
    def nodes(self):
        return self.dataGen(self.getNode)