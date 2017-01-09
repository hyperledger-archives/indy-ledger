from ledger.stores.chunked_file_store import ChunkedFileStore
from ledger.stores import store_utils


class ChunkedTextFileStore(ChunkedFileStore):

    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 chunkSize=1000):
        super().__init__(dbDir, dbName, isLineNoKey,
                         storeContentHash, chunkSize)
        self._initDB(dbDir, dbName)

    def _initDB(self, dbDir, dbName):
        super()._initDB(dbDir, dbName)

    def _getLines(self, dbFile):
        return store_utils.cleanLines(dbFile)
