import os
from hashlib import sha256


class FileStore:
    """
    A file based implementation of a key value store.
    """

    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True):
        """
        :param dbDir: The directory where the file storing the data would be
        present
        :param dbName: The name of the file that is used to store the data
        :param isLineNoKey: If false then each line has the key followed by a
        delimiter followed by the value
        :param storeContentHash: Whether to store a hash of the value or not.
        Storing hash can make it really fast to compare the value for equality
        """
        self.isLineNoKey = isLineNoKey
        self.storeContentHash = storeContentHash

    def _prepareDBLocation(self, dbDir, dbName):
        self.dbDir = dbDir
        self.dbName = dbName
        if not os.path.exists(self.dbDir):
            os.makedirs(self.dbDir)

    def _initDB(self, dbDir, dbName):
        self._prepareDBLocation(dbDir, dbName)

    # noinspection PyUnresolvedReferences
    def put(self, value, key=None):
        # If line no is not treated as key then write the key and then the
        # delimiter
        if not self.isLineNoKey:
            if key is None:
                raise ValueError("Key must be provided for storing the value")
            self.dbFile.write(key)
            self.dbFile.write(self.delimiter)

        self.dbFile.write(value)

        if self.storeContentHash:
            self.dbFile.write(self.delimiter)
            if isinstance(value, str):
                value = value.encode()
            hexedHash = sha256(value).hexdigest()
            self.dbFile.write(hexedHash)
        self.dbFile.write(self.lineSep)
        self.dbFile.flush()

    def get(self, key):
        for k, v in self.iterator():
            if k == key:
                return v

    # noinspection PyUnresolvedReferences
    def iterator(self,
                 includeKey=True,
                 includeValue=True,
                 prefix=None,
                 dbFile=None):
        if not (includeKey or includeValue):
            raise ValueError("At least one of includeKey or includeValue "
                             "should be true")
        if includeKey and includeValue:
            return self._keyValueIterator(prefix, dbFile)
        elif includeValue:
            return self._valueIterator(prefix, dbFile)
        else:
            return self._keyIterator(prefix, dbFile)

    def _keyIterator(self, prefix=None, dbFile=None):
        return self._baseIterator(prefix, True, False, dbFile)

    def _valueIterator(self, prefix=None, dbFile=None):
        return self._baseIterator(prefix, False, True, dbFile)

    def _keyValueIterator(self, prefix=None, dbFile=None):
        return self._baseIterator(prefix, True, True, dbFile)

    # noinspection PyUnresolvedReferences
    def _baseIterator(self, prefix, returnKey: bool, returnValue: bool, dbFile: str=None):
        # Move to the beginning of file
        dbFile = dbFile or self.dbFile
        dbFile.seek(0)
        lines = self._getLines(dbFile)

        i = 1
        for line in lines:
            if self.isLineNoKey:
                k = str(i)
                v = line
                i += 1
            else:
                k, v = line.split(self.delimiter, 1)
            if returnValue:
                if self.storeContentHash:
                    value, _ = v.rsplit(self.delimiter, 1)
                else:
                    value = v
            if not prefix or k.startswith(prefix):
                if returnKey and returnValue:
                    yield (k, value)
                elif returnKey:
                    yield k
                elif returnValue:
                    yield value

    def _getLines(self, dbFile):
        raise NotImplementedError()

    @property
    def lastKey(self):
        # TODO use the efficient way of seeking to the end and moving back till
        # 2nd newline(1 st newline would be encountered immediately until its a
        # blank file) is encountered and after newline read ahead till the
        # delimiter or split the read string till now on delimiter
        k = None
        for k, v in self.iterator():
            pass
        return k

    @property
    def numKeys(self):
        return sum(1 for l in self.iterator())

    # noinspection PyUnresolvedReferences
    def close(self):
        self.dbFile.close()

    # noinspection PyUnresolvedReferences
    def reset(self):
        self.dbFile.truncate(0)
