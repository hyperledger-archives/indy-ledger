from immutable_store.mongodb_ledger import MongoDBLedger

# TODO These tests will write to local mongoDB, must use a DBUnit instead.
# TODO afterAll must drop the databases

store = MongoDBLedger("testLedger", "ledger")


def testLedgerPersistsOneRecord():
    key = 'client_id'
    record = {key: 1}
    store.append(record)
    assert record[key] == store.findTxnByProperties(record)[0][key]


def testLedgerPersistsMultipleRecords():
    pass


def testLedgerCanPersistAFullSubTree():
    pass


def testLedgerCanPersistANonFullSubTree():
    pass


def testLedgerCanLoadUpAValidMerkleTree():
    pass


# Advanced test
def testConstructSparseMerkleFromCompactMerkleTreeData():
    pass


def testCleanup():
    store._ledger.drop()
