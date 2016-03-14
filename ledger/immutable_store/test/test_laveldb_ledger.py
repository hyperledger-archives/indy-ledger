from ledger.immutable_store.leveldb_ledger import LevelDBLedger

store = LevelDBLedger("/tmp/testleveldb")


def testLevelDBPersistsRecord():
    key = "client_id"
    record = {key: 1}
    store.append(record)
    assert record[key] == store.find(key)