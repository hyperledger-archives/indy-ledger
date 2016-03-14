import asyncio
from collections import namedtuple

from ledger.immutable_store.ledger import Ledger
from ledger.immutable_store.leveldb_ledger import LevelDBLedger
from ledger.immutable_store.merkle import CompactMerkleTree
store = LevelDBLedger("/tmp/testleveldb")


def testLevelDBPersistsRecord():
    serialno = 2
    record = {"my code": "my logics"}
    store.append(serialno, record)
    assert record == store.get(serialno)


def testTearDownLevelDB():
    store.drop()

def testTxnPersistence():
    Reply = namedtuple('Reply', ['viewNo', 'reqId', 'result'])
    loop = asyncio.get_event_loop()
    ledger_db = LevelDBLedger("/tmp/testLedgerPers")
    ldb = Ledger(CompactMerkleTree(), ledger_db)
    async def go():
        clientId = "testClientId"
        txnId = "txnId"
        reply = Reply(1, 1, "theresult")
        await ldb.insertTxn(clientId, reply, txnId)
        txn_in_db = await ldb.getTxn(clientId, reply.reqId)
        assert txn_in_db == reply

    loop.run_until_complete(go())
    loop.close()