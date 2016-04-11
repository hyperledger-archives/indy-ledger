import os

from ledger.immutable_store.stores.file_store import FileStore


class TextFileStore(FileStore):
    def __init__(self, dbDir, dbName, keyIsLineNo: bool=False):
        # This is the separator between key and value
        self.delimiter = "\t"
        self.lineSep = os.linesep
        self.keyIsLineNo = keyIsLineNo
        self._initDB(dbDir, dbName)

    def _initDB(self, dbDir, dbName):
        super()._initDB(dbDir, dbName)
        self.dbPath = os.path.join(dbDir, dbName)
        self._dbFile = open(self.dbPath, mode="a+")

    def _getLines(self):
        return (line.strip(self.lineSep) for line in
                self._dbFile if len(line.strip(self.lineSep)) != 0)
