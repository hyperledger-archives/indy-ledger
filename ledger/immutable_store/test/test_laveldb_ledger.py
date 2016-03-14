from ledger.immutable_store.leveldb_ledger import LevelDBLedger

store = LevelDBLedger("/tmp/testleveldb")


def testLevelDBPersistsRecord():
    serialno = 2
    record = {"my code": "my logics"}
    store.append(serialno, record)
    assert record == store.get(serialno)

def testTearDownLevelDB():
    store.drop()