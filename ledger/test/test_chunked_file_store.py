import os, random
import pytest

from ledger.stores.text_file_store import TextFileStore


def countLines(fname) -> int:
    with open(fname) as f:
        return sum(1 for _ in f)


def getValue(key) -> str:
    return str(key) + ' Some data'

chunkSize = 2
dataSize = 100


@pytest.fixture(scope="module")
def chunkedTextFileStore() -> TextFileStore:
    return TextFileStore('/tmp', 'chunked_data', True, True, chunkSize)


@pytest.fixture(scope="module")
def populatedChunkedFileStore(chunkedTextFileStore) -> TextFileStore:
    store = chunkedTextFileStore
    store.reset()
    dirPath = '/tmp/chunked_data'
    for i in range(1, dataSize + 1):
        store.put(getValue(i), str(i))
    assert len(os.listdir(dirPath)) == dataSize / chunkSize
    assert all(countLines(dirPath + os.path.sep + f) == chunkSize
               for f in os.listdir(dirPath))
    store.close()
    return store


def testWriteToNewFileOnceChunkSizeIsReached(populatedChunkedFileStore):
    pass


def testRandomRetrievalFromChunkedFiles(populatedChunkedFileStore):
    store = populatedChunkedFileStore
    store.open()
    key = random.randrange(1, dataSize+1)
    value = getValue(key)
    assert store.get(key) == value

