import asyncio
from collections import namedtuple

import plyvel

from ledger.immutable_store.ledger import Ledger
from ledger.immutable_store.merkle import CompactMerkleTree

levelDBDir = "/tmp/testleveldb"

def testTxnPersistence():
    Reply = namedtuple('Reply', ['viewNo', 'reqId', 'result'])
    loop = asyncio.get_event_loop()
    ldb = Ledger(CompactMerkleTree(), levelDBDir)
    async def go():
        clientId = "testClientId"
        txnId = "txnId"
        reply = Reply(1, 1, "theresult")
        await ldb.append(clientId, reply, txnId)
        txn_in_db = await ldb.get(clientId, reply.reqId)
        assert txn_in_db == reply
        assert ldb.size() == 1
        ldb.stop()

    loop.run_until_complete(go())
    loop.close()
    plyvel.destroy_db(levelDBDir)