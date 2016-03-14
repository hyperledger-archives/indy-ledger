from pymongo import MongoClient

from ledger.immutable_store.store import ImmutableStore, F


# TODO This class currently connects to a local instance of MongoDB. Must be
# configurable to connect to remote dbs.
class MongoDBLedger(ImmutableStore):
    """
    Persistence Adapter for Merkle Trees that uses MongoDB.
    """

    def __init__(self, dbName: str, collectionName: str):
        self._client = MongoClient()
        self._db = self._client[dbName]
        self._ledger = self._db[collectionName]

    def append(self, record):
        self.validate(record)
        return self._ledger.insert_one(record)

    def validate(self, record: dict) -> bool:
        """
        Checks whether all the properties in the record are valid.

        :param record:
        :return:
        """
        assert all(x in [x for x in dir(F) if not x.startswith('__')]
                   for x in record.keys())

    def get(self, seqNo: int):
        return self._ledger.find_one({F.seq_no.name: seqNo})

    def find(self, propertyMap: dict):
        return self._ledger.find(propertyMap)

    def getAll(self):
        return self._ledger.find().sort(F.seq_no.name, 1)
