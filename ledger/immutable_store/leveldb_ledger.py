import os

import plyvel

from ledger.immutable_store.store import ImmutableStore


class LevelDBLedger(ImmutableStore):
    def __init__(self, dir: str):
        self._db = plyvel.DB(dir,
                             create_if_missing=True)

    def append(self, record):
        self.validate(record)
        key = list(record.keys())[0]
        value = list(record.values())[0]
        print("<<<<<<<< {}, {}".format(key, value))
        x = self._db.put(bytes('{}'.format(key).encode('utf-8')),
                         bytes('{}'.format(value).encode('utf-8')))
        print(">>>>>>>>> {}".format(x))

    def validate(self, record: dict) -> bool:
        assert True

    def find(self, key):
        return (self._db.get(bytes('{}'.format(key).encode('utf-8')))).decode('utf-8')
