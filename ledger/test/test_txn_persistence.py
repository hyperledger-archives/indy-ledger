import asyncio
import time
from collections import OrderedDict, namedtuple
from tempfile import TemporaryDirectory

from ledger.ledger import Ledger
from ledger.serializers.compact_serializer import CompactSerializer
from ledger.compact_merkle_tree import CompactMerkleTree

Reply = namedtuple("REPLY", ['result'])


def testTxnPersistence():
    with TemporaryDirectory() as tdir:
        loop = asyncio.get_event_loop()
        fields = OrderedDict([
                ("identifier", (str, str)),
                ("reqId", (str, int)),
                ("txnId", (str, str)),
                ("txnTime", (str, float)),
                ("txnType", (str, str)),
            ])
        ldb = Ledger(CompactMerkleTree(), tdir,
                     serializer=CompactSerializer(fields=fields))

        def go():
            identifier = "testClientId"
            txnId = "txnId"
            reply = Reply(result={
                "identifier": identifier,
                "reqId": 1,
                "txnId": txnId,
                "txnTime": time.time(),
                "txnType": "buy"
            })
            sizeBeforeInsert = ldb.size
            ldb.append(reply.result)
            txn_in_db = ldb.get(identifier=identifier,
                                reqId=reply.result['reqId'])
            assert txn_in_db == reply.result
            assert ldb.size == sizeBeforeInsert + 1
            ldb.reset()
            ldb.stop()

        go()
        loop.close()
