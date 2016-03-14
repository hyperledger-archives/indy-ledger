import plyvel

from ledger.immutable_store.store import ImmutableStore


class LevelDBLedger(ImmutableStore):
    def __init__(self, dir: str):
        self.dir = dir
        self._db = plyvel.DB(dir, create_if_missing=True)
        self._reply = self._db.prefixed_db(b'reply')
        self._processedReq = self._db.prefixed_db(b'processedReq')

    def insertProcessedReq(self, clientId, reqId, serial_no):
        key = bytes((clientId + "-" + str(reqId)), 'utf-8')
        value = bytes(str(serial_no), 'utf-8')
        self._processedReq.put(key, value)

    def getProcessedReq(self, clientId, reqId):
        key = bytes((clientId + "-" + str(reqId)), 'utf-8')
        serialNo = self._processedReq.get(key)
        if serialNo:
            return serialNo.decode('utf-8')
        else:
            return serialNo

    def append(self, serialno, record):
        key = bytes(str(serialno).encode('utf-8'))
        self._reply.put(key, bytes(str(record), 'utf-8'))

    def get(self, serialno):
        key = str(serialno).encode('utf-8')
        value = self._reply.get(key)
        if value:
            return eval(value.decode('utf-8'))
        else:
            return value

    def getAll(self):
        return self._reply.iterator()

    def drop(self):
        self._db.close()
        plyvel.destroy_db(self.dir)

    def lastCount(self):
        for key, _ in self._reply.iterator(reverse=True):
            return int(key.decode('utf-8'))
        return 0

    def start(self, loop):
        pass

    def stop(self):
        self.drop()
