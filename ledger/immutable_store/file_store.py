import os


class FileStore:
    def __init__(self, dbDir, dbName):
        # This is the separator between key and value
        self.delimiter = b"\t"
        # TODO: This line separator might conflict with some data format.
        # So prefix the value data in the file with size and only read those
        # number of bytes.
        self.lineSep = b'\n\x07\n\x01'
        self._initDB(dbDir, dbName)

    def _isBytes(self, arg):
        return isinstance(arg, bytes) or isinstance(arg, bytearray)

    def _prepareDBLocation(self, dbDir, dbName):
        self.dbDir = dbDir
        self.dbName = dbName
        if not os.path.exists(self.dbDir):
            os.makedirs(self.dbDir)

    def _initDB(self, dbDir, dbName):
        self._prepareDBLocation(dbDir, dbName)
        self.dbPath = os.path.join(dbDir, "{}.bin".format(dbName))
        self._dbFile = open(self.dbPath, mode="a+b", buffering=0)

    def put(self, key, value):
        if not (self._isBytes(key) or self._isBytes(value)):
            raise ValueError("key and value need to be bytes-like object")
        self._dbFile.write(key)
        self._dbFile.write(self.delimiter)
        self._dbFile.write(value)
        self._dbFile.write(self.lineSep)

    def get(self, key):
        if not self._isBytes(key):
            raise TypeError("key needs to be a bytes-like object")
        for k, v in self.iterator():
            if k == key:
                return v

    def _keyIterator(self, lines, prefix=None):
        for line in lines:
            k, v = line.split(self.delimiter, 1)
            if not prefix or k.startswith(prefix):
                yield k

    def _valueIterator(self, lines, prefix=None):
        for line in lines:
            k, v = line.split(self.delimiter, 1)
            if not prefix or k.startswith(prefix):
                yield v

    def _keyValueIterator(self, lines, prefix=None):
        for line in lines:
            k, v = line.split(self.delimiter, 1)
            if not prefix or k.startswith(prefix):
                yield (k, v)

    def iterator(self, include_key=True, include_value=True, prefix=None):
        if not (include_key or include_value):
            raise ValueError("At least one include_key or include_value "
                             "should be true")
        if prefix and not self._isBytes(prefix):
            raise TypeError("prefix needs to be a bytes-like object")

        self._dbFile.seek(0)
        lines = (line.strip(self.lineSep) for line in self._dbFile.read().split(self.lineSep)
                 if line.strip(self.lineSep) != b'')
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

    def close(self):
        self._dbFile.close()

    def reset(self):
        self._dbFile.truncate(0)
