import asyncio
from collections import namedtuple
from tempfile import TemporaryDirectory

import pytest

from ledger.immutable_store.ledger import Ledger
from ledger.immutable_store.merkle import CompactMerkleTree


@pytest.mark.skipif(True, reason="Ledger implementation changing; need to come "
                                 "back to this once it's stabilized")
def testTxnPersistence():
    with TemporaryDirectory() as tdir:
        Reply = namedtuple('Reply', ['viewNo', 'reqId', 'result'])
        loop = asyncio.get_event_loop()
        ldb = Ledger(CompactMerkleTree(), tdir)
        async def go():
            identifier = "testClientId"
            txnId = "txnId"
            reply = Reply(1, 1, "theresult")
            sizeBeforeInsert = ldb.size
            await ldb.append(identifier, reply, txnId)
            assert ldb.getBySerialNo(0)['STH']['tree_size'] == 1
            txn_in_db = await ldb.get(identifier, reply.reqId)
            assert txn_in_db == reply.result
            assert ldb.size == sizeBeforeInsert + 1
            ldb.reset()
            ldb.stop()

        loop.run_until_complete(go())
        loop.close()
