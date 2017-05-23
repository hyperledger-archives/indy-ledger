import os
import pytest
from ledger.stores.chunked_file_store import ChunkedFileStore
from ledger.stores.text_file_store import TextFileStore


def test_equality_to_text_file_store(tmpdir):
    """
    This test verifies that TextFileStore and ChunkedFileStore behave equally
    """

    chunkSize=3

    isLineNoKey = True
    storeContentHash = True
    ensureDurability = True
    dbDir = str(tmpdir)

    chunkedStore = ChunkedFileStore(dbDir=dbDir,
                                    dbName="chunked_data",
                                    isLineNoKey=isLineNoKey,
                                    storeContentHash=storeContentHash,
                                    chunkSize=chunkSize,
                                    ensureDurability=ensureDurability,
                                    chunkStoreConstructor=TextFileStore)

    textStore = TextFileStore(dbDir=dbDir,
                              dbName="text_data",
                              isLineNoKey=isLineNoKey,
                              storeContentHash=storeContentHash,
                              ensureDurability=ensureDurability)

    for i in range(1, 5 * chunkSize):
        value = str(i)
        chunkedStore.put(value)
        textStore.put(value)
        assert list(chunkedStore.iterator()) == \
               list(textStore.iterator())