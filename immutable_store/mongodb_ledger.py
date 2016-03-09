from pymongo import MongoClient


# TODO Create an interface for persistence adapters like this one.
class MongoDBLedger():
    """
    Persistence Adapter for Merkle Trees that uses MongoDB.
    """
    def __init__(self, dbName: str, collectionName: str):
        # TODO Create db if not exists
        self._client = MongoClient()
        self._db = self._client[dbName]
        self.ledger = self._db[collectionName]
        # TODO The db name or table name may need to be changed

    def persist(self, record):
        # TODO Some validation maybe required here.
        return self.ledger.insert_one(record)

    # TODO Add some query methods here.

