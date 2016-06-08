"""
Stores chunks of data into separate files. The chunking of data is
determined by the `chunkSize` parameter.
Every instance of ChunkedFileStore maintains its own directory for
storing the chunked data files.
The filename of each chunked file is a number. The number is
a multiple of the chunkSize.
An example file name would be `5000`
So, in this case, if we know that the chunkSize is 1000, we can say
that this is the fifth chunked file. ChunkSize can simply be
determined by taking the difference of the numbers of two
consecutively written files.
"""

import os

from ledger.stores.file_store import FileStore


# TODO Need to remove code duplicated between this and super
class ChunkedFileStore(FileStore):
    def __init__(self, dbDir, dbName, isLineNoKey: bool=False,
                 storeContentHash: bool=True, chunkSize=1000):
        FileStore.__init__(self, dbDir, dbName, isLineNoKey,
                           storeContentHash)
        self.chunkSize = chunkSize
        self.lineNum = 1
        self.dbFile = None
        # This directory stores the data written into multiple files.
        self.dataDir = os.path.join(dbDir, dbName)

    def _initDB(self, dataDir, dbName):
        self._prepareDBLocation(dataDir, dbName)
        # Open the most recent file in append mode
        self._resetDbFile()

    def _resetDbFile(self):
        self.dbFile = open(self._getLatestFile(), mode="a+")

    def _getLatestFile(self):
        return os.path.join(self.dataDir, self._findLatestFile())

    def _prepareDBLocation(self, dbDir, dbName):
        self.dbName = dbName
        if not os.path.exists(self.dataDir):
            os.makedirs(self.dataDir)

    def _findLatestFile(self):
        # This assumes that the fileNames are in the format
        #  file_name_chunkNumber.
        fileNos = [int(fn) for fn in os.listdir(self.dataDir)]
        return str(max(fileNos) if fileNos else self.chunkSize)

    def _createNewDbFile(self):
        self.dbFile.close()
        currentChunk = int(self.dbFile.name)
        nextChunk = currentChunk + self.chunkSize
        self.dbFile = open(os.path.join(self.dataDir, str(nextChunk)),
                           mode="a+")

        # noinspection PyUnresolvedReferences
    def put(self, value, key=None):
        # If line no is not treated as key then write the key and then the
        # delimiter
        if self.lineNum > self.chunkSize:
            self._createNewDbFile()
            self.lineNum = 1
        super(ChunkedFileStore, self).put(value, key)

    def get(self, key):
        remainder = int(key) % self.chunkSize
        fileNo = int(key) - remainder + self.chunkSize if remainder else key
        keyToCompare = str(remainder if self.isLineNoKey else key)
        for k, v in self.iterator(
                dbFile=open(os.path.join(self.dataDir, str(fileNo)),
                            mode="a+")):
            if k == keyToCompare:
                return v

    # noinspection PyUnresolvedReferences
    def reset(self):
        """
        Clear all data in file storage.
        """
        for f in os.listdir(self.dataDir):
            os.remove(os.path.join(self.dataDir, f))
        self._resetDbFile()

    def _getLines(self, dbFile):
        raise NotImplementedError()
