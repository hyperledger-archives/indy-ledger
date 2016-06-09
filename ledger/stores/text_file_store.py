import os

from ledger.stores.chunked_file_store import ChunkedFileStore


class TextFileStore(ChunkedFileStore):
    def __init__(self, dbDir, dbName, isLineNoKey: bool=False,
                 storeContentHash: bool=True, chunkSize=1000):
        ChunkedFileStore.__init__(self, dbDir, dbName, isLineNoKey,
                                  storeContentHash, chunkSize)
        self.delimiter = "\t"  # separator between key and value
        self.lineSep = os.linesep
        self._initDB(dbDir, dbName)

    def _initDB(self, dbDir, dbName):
        super()._initDB(dbDir, dbName)

    def _getLines(self, dbFile):
        return (line.strip(self.lineSep) for line in
                dbFile if len(line.strip(self.lineSep)) != 0)
