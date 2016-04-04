import os
from hashlib import sha256


class FileStore:
    def __init__(self, dbDir, dbName, keyIsLineNo: bool=False):
        raise NotImplementedError

    def _prepareDBLocation(self, dbDir, dbName):
        self.dbDir = dbDir
        self.dbName = dbName
        if not os.path.exists(self.dbDir):
            os.makedirs(self.dbDir)

    def _initDB(self, dbDir, dbName):
        self._prepareDBLocation(dbDir, dbName)

    # noinspection PyUnresolvedReferences
    def put(self, key, value):
        if not self.keyIsLineNo:
            self._dbFile.write(key)
            self._dbFile.write(self.delimiter)
        self._dbFile.write(value)
        self._dbFile.write(self.delimiter)
        # TODO: Consider storing hash optional. The challenge is that then
        # during parsing it has to be checked whether hash is present or not.
        # That can be a trouble id the delimiter is also present inside the value
        if isinstance(value, str):
            value = value.encode()
        hash = sha256(value).hexdigest()
        self._dbFile.write(hash)
        self._dbFile.write(self.lineSep)
        self._dbFile.flush()

    def get(self, key):
        for k, v in self.iterator():
            if k == key:
                return v

    # noinspection PyUnresolvedReferences
    def _keyIterator(self, lines, prefix=None):
        i = 0
        for line in lines:
            if self.keyIsLineNo:
                k = str(i)
                i += 1
            else:
                k, v = line.split(self.delimiter, 1)
            if not prefix or k.startswith(prefix):
                yield k

    # noinspection PyUnresolvedReferences
    def _valueIterator(self, lines, prefix=None):
        i = 0
        for line in lines:
            if self.keyIsLineNo:
                k = str(i)
                v = line
                i += 1
            else:
                k, v = line.split(self.delimiter, 1)
            value, hash = v.rsplit(self.delimiter, 1)
            if not prefix or k.startswith(prefix):
                yield value

    # noinspection PyUnresolvedReferences
    def _keyValueIterator(self, lines, prefix=None):
        i = 0
        for line in lines:
            if self.keyIsLineNo:
                k = str(i)
                v = line
                i += 1
            else:
                k, v = line.split(self.delimiter, 1)
            value, hash = v.rsplit(self.delimiter, 1)
            if not prefix or k.startswith(prefix):
                yield (k, value)

    def _getLines(self):
        raise NotImplementedError()

    # noinspection PyUnresolvedReferences
    def iterator(self, include_key=True, include_value=True, prefix=None):
        if not (include_key or include_value):
            raise ValueError("At least one include_key or include_value "
                             "should be true")
        self._dbFile.seek(0)
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
        self._dbFile.close()

    # noinspection PyUnresolvedReferences
    def reset(self):
        self._dbFile.truncate(0)
