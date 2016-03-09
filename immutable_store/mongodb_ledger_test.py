from immutable_store.mongodb_ledger import MongoDBLedger

# TODO These tests will write to local mongoDB, must use a DBUnit instead.

ledger = MongoDBLedger("testLedger", "ledger")


def testLedgerPersistsOneRecord():
    ret = ledger.persist(
        {"key": "no validation yet. Can insert anything"})
    print(ret)
    assert (ret is not None)


def testLedgerPersistsMultipleRecords():
    pass


def testLedgerCanPersistAFullSubTree():
    pass


def testLedgerCanPersistANonFullSubTree():
    pass


def testLedgerCanLoadUpAValidMerkleTree():
    pass

