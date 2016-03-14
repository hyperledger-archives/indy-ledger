from ledger.immutable_store.mongodb_ledger import MongoDBLedger

# TODO These tests will write to local mongoDB, must use a DBUnit instead.
# TODO afterAll must drop the databases

store = MongoDBLedger("testLedger", "ledger")


def testLedgerPersistsOneRecord():
    key = 'client_id'
    record = {key: 1}
    store.append(record)
    assert record[key] == store.find(record)[0][key]


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


# TODO An in-memory store can be used instead of using this tear down logic.
def testTearDown():
    store._ledger.drop()
