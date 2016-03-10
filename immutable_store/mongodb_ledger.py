from pymongo import MongoClient

from immutable_store.store import ImmutableStore, StoreType, \
    Properties


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

    def storeType(self):
        return StoreType.nosql

    def append(self, record):
        self.validate(record)
        return self._ledger.insert_one(record)

    def validate(self, record: dict) -> bool:
        """
        Checks whether all the properties in the record are valid.

        :param record:
        :return:
        """
        assert all(x in [x for x in dir(Properties) if not x.startswith('__')]
                   for x in record.keys())

    def getTxnBySeqNo(self, seqNo: int):
        return self._ledger.find_one({Properties.seq_no.name: seqNo})

    def findTxnByProperties(self, propertyMap: dict):
        return self._ledger.find(propertyMap)

    def getAllTxns(self):
        return self._ledger.find().sort(Properties.seq_no.name, 1)