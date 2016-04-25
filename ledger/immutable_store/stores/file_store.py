import os
from hashlib import sha256


class FileStore:
    def __init__(self, dbDir, dbName, keyIsLineNo: bool=False,
                 storeContentHash: bool=True):
        self.keyIsLineNo = keyIsLineNo
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
        if not self.keyIsLineNo:
            if key is None:
                raise ValueError("Key must be provided for storing the value")
            self.dbFile.write(key)
            self.dbFile.write(self.delimiter)
        self.dbFile.write(value)
        # TODO: Consider storing hash optional. The challenge is that then
        # during parsing it has to be checked whether hash is present or not.
        # That can be a trouble if the delimiter is also present inside the value
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

    def _keyIterator(self, lines, prefix=None):
        return self._baseIterator(lines, prefix, True, False)

    def _valueIterator(self, lines, prefix=None):
        return self._baseIterator(lines, prefix, False, True)

    def _keyValueIterator(self, lines, prefix=None):
        return self._baseIterator(lines, prefix, True, True)

    # noinspection PyUnresolvedReferences
    def _baseIterator(self, lines, prefix, returnKey, returnValue):
        i = 0
        for line in lines:
            if self.keyIsLineNo:
                k = str(i)
                v = line
                i += 1
            else:
                k, v = line.split(self.delimiter, 1)
            if returnValue:
                if self.storeContentHash:
                    value, hash = v.rsplit(self.delimiter, 1)
                else:
                    value = v
            if not prefix or k.startswith(prefix):
                if returnKey and returnValue:
                    yield (k, value)
                elif returnKey:
                    yield k
                elif returnValue:
                    yield value

    def _getLines(self):
        raise NotImplementedError()

    # noinspection PyUnresolvedReferences
    def iterator(self, include_key=True, include_value=True, prefix=None):
        if not (include_key or include_value):
            raise ValueError("At least one of include_key or include_value "
                             "should be true")
        self.dbFile.seek(0)
        lines = self._getLines()
        if include_key and include_value:
            return self._keyValueIterator(lines, prefix=prefix)
        elif include_value:
            return self._valueIterator(lines, prefix=prefix)
        else:
            return self._keyIterator(lines, prefix=prefix)

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
