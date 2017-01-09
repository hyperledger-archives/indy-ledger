import itertools
import os

import pytest

from ledger.test.helper import makeTempdir


@pytest.fixture(scope="session")
def counter():
    return itertools.count()


@pytest.fixture()
def tempdir(tmpdir_factory, counter):
    return makeTempdir(tmpdir_factory, counter)
