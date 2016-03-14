import json
import os

import plyvel

from ledger.immutable_store.store import ImmutableStore


class LevelDBLedger(ImmutableStore):
    def __init__(self, dir: str):
        self.dir = dir
        self._db = plyvel.DB(dir, create_if_missing=True)

    def append(self, serialno, record):
        key = bytes(str(serialno).encode('utf-8'))
        self._db.put(key, bytes(str(record), 'utf-8'))


    def get(self, serialno):
        key = str(serialno).encode('utf-8')
        value = self._db.get(key)
        record = eval(value.decode('utf-8'))
        return record

    def getAll(self):
        return self._db.iterator()

    def drop(self):
        self._db.close()
        plyvel.destroy_db(self.dir)

    def lastCount(self):
        for key, _ in self._db.iterator(reverse=True):
            return int(key.decode('utf-8'))
        return 0
