import os


class FileStore:
    def _prepareDBLocation(self, dbDir, dbName):
        self.dbDir = dbDir
        self.dbName = dbName
        if not os.path.exists(self.dbDir):
            os.makedirs(self.dbDir)

    def _initDB(self, dbDir, dbName):
        self._prepareDBLocation(dbDir, dbName)

    def put(self, key, value):
        self._dbFile.write(key)
        self._dbFile.write(self.delimiter)
        self._dbFile.write(value)
        self._dbFile.write(self.lineSep)

    def get(self, key):
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

    def _getLines(self):
        raise NotImplementedError()

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

    def close(self):
        self._dbFile.close()

    def reset(self):
        self._dbFile.truncate(0)