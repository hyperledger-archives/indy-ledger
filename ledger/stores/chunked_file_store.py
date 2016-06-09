"""
Stores chunks of data into separate files. The chunking of data is
determined by the `chunkSize` parameter. Each chunk of data is written to a
different file.
The naming convention of the files is such that the starting number of each
chunk is the file name, i.e. for a chunkSize of 1000, the first file would be
1, the second 1001 etc.
Every instance of ChunkedFileStore maintains its own directory for
storing the chunked data files.
"""

import os
from typing import List

from ledger.stores.file_store import FileStore


class ChunkedFileStore(FileStore):

    firstFileName = '1'

    def __init__(self, dbDir, dbName, isLineNoKey: bool = False,
                 storeContentHash: bool = True, chunkSize=1000):
        FileStore.__init__(self, dbDir, dbName, isLineNoKey,
                           storeContentHash)
        self.chunkSize = chunkSize
        self.lineNum = 1
        self.dbFile = None
        # This directory stores the data written into multiple files.
        self.dataDir = os.path.join(dbDir, dbName)

    def _initDB(self, dataDir, dbName) -> None:
        self._prepareDBLocation(dataDir, dbName)
        self._resetDbFile()

    def _resetDbFile(self) -> None:
        """
        Open the most recent file in append mode
        """
        self.dbFile = open(self._getLatestFile(), mode="a+")

    def _getLatestFile(self) -> str:
        return os.path.join(self.dataDir, self._findLatestFile())

    def _findLatestFile(self) -> str:
        """
        Determine which file is the latest and return its file name.
        """
        fileNos = [int(fn) for fn in os.listdir(self.dataDir)]
        return str(max(fileNos) if fileNos else ChunkedFileStore.firstFileName)

    def _prepareDBLocation(self, dbDir, dbName) -> None:
        self.dbName = dbName
        if not os.path.exists(self.dataDir):
            os.makedirs(self.dataDir)

    def _createNewDbFile(self) -> None:
        """
        Create a new file to write the next chunk of data into.
        """
        self.dbFile.close()
        currentChunk = int(self.dbFile.name.split(os.path.sep)[-1])
        nextChunk = currentChunk + self.chunkSize
        self.dbFile = open(os.path.join(self.dataDir, str(nextChunk)),
                           mode="a+")

    def put(self, value, key=None) -> None:
        """
        Adds a chunking behavior to FileStore's put method.
        Writes to a new file when a chunk is filled.
        """
        if self.lineNum > self.chunkSize:
            self._createNewDbFile()
            self.lineNum = 1
        self.lineNum += 1
        super(ChunkedFileStore, self).put(value, key)

    def get(self, key) -> str:
        """
        Determines the file to retrieve the data from and retrieves the data.
        """
        remainder = int(key) % self.chunkSize
        addend = int(ChunkedFileStore.firstFileName)
        fileNo = int(key) - remainder + addend if remainder \
            else key - self.chunkSize + addend
        keyToCompare = str(self.chunkSize if not remainder else remainder)
        chunkedFile = open(os.path.join(self.dataDir, str(fileNo)), mode="a+")
        for k, v in self.iterator(dbFile=chunkedFile):
            if k == keyToCompare:
                chunkedFile.close()
                return v

    def reset(self) -> None:
        """
        Clear all data in file storage.
        """
        for f in os.listdir(self.dataDir):
            os.remove(os.path.join(self.dataDir, f))
        self._resetDbFile()

    def _getLines(self, dbFile) -> List[str]:
        raise NotImplementedError()

    def open(self) -> None:
        self.dbFile = open(self._getLatestFile(), mode="a+")
