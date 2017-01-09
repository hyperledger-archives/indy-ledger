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
    # TODO: This should not extends file store but have several file stores,
    # one for each chunk. It should take an argument to whether each chunk is
    # binary or text, either all chunks are `BinaryFileStore`s or
    # `TextFileStore`s

    firstFileName = '1'

    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 chunkSize: int=1000,
                 ensureDurability: bool=True):
        FileStore.__init__(self, dbDir, dbName, isLineNoKey, storeContentHash,
                           ensureDurability)
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
        last = ChunkedFileStore.\
            _fileNameToChunkIndex(ChunkedFileStore.firstFileName)
        for fileName in os.listdir(self.dataDir):
            index = ChunkedFileStore._fileNameToChunkIndex(fileName)
            if index is not None and index > last:
                last = index
        return str(last)

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
        self.dbFile = open(
            os.path.join(self.dataDir, str(nextChunk)),
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
        chunkedFile = os.path.join(self.dataDir, str(fileNo))
        for k, v in self.iterator(dbFile=chunkedFile):
            if k == keyToCompare:
                return v

    def reset(self) -> None:
        """
        Clear all data in file storage.
        """
        for f in os.listdir(self.dataDir):
            os.remove(os.path.join(self.dataDir, f))
        self._resetDbFile()

    def _getLines(self, dbFile) -> List[str]:
        # TODO: This needs to be implemented.
        raise NotImplementedError

    def _baseIterator(self, prefix, returnKey: bool, returnValue: bool,
                      dbFile: str=None):
        if dbFile:
            with open(dbFile, 'r') as fil:
                yield from super()._baseIterator(self._getLines(fil), prefix,
                                                 returnKey, returnValue)
        else:
            for fil in os.listdir(self.dataDir):
                with open(os.path.join(self.dataDir, fil), 'r') as dbFile:
                    yield from super()._baseIterator(self._getLines(dbFile),
                                                     prefix, returnKey,
                                                     returnValue)

    def open(self) -> None:
        self.dbFile = open(self._getLatestFile(), mode="a+")


    @staticmethod
    def _fileNameToChunkIndex(fileName):
        try:
            return int(fileName)
        except:
            return None
