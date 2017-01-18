import os

from ledger.stores.file_store import FileStore


class TextFileStore(FileStore):
    def __init__(self, dbDir, dbName, isLineNoKey: bool=False,
                 storeContentHash: bool=True, ensureDurability: bool=True):
        # This is the separator between key and value
        self.delimiter = "\t"
        self.lineSep = os.linesep
        super().__init__(dbDir, dbName, isLineNoKey, storeContentHash,
                         ensureDurability)
        self._initDB(dbDir, dbName)

    def _initDB(self, dbDir, dbName):
        super()._initDB(dbDir, dbName)
        self.dbPath = os.path.join(os.path.expanduser(dbDir), dbName)
        self.dbFile = open(self.dbPath, mode="a+")

    def _getLines(self):
        return (line.strip(self.lineSep) for line in
                self.dbFile if len(line.strip(self.lineSep)) != 0)
