from abc import abstractmethod, abstractproperty, ABCMeta
from enum import Enum


class F(Enum):
    client_id = 1
    request_id = 2
    STH = 3
    leaf_data = 4
    leaf_data_hash = 5
    created = 6
    added_to_tree = 7
    audit_info = 8
    seq_no = 9


class ImmutableStore:
    """
    Interface for immutable stores.
    An immutable store is any storage system (database, flatfile, in-memory,
    etc.). It stores the transaction data and the relevant info from the
    Merkle Tree.
    """

    @abstractmethod
    def append(self, key, value) -> bool:
        """
        Append a new record to the immutable ledger
        :param record: The record to append
        :return: True if appending is successful
        """

    @abstractmethod
    def validate(self, record) -> bool:
        """
        Check whether the attributes in the record match the data model.

        :param record: the record to validate
        :return: True if valid
        """

    @abstractmethod
    def get(self, seqNo: int):
        """

        :param seqNo:
        :return:
        """

    @abstractmethod
    def find(self, paramAndValues):
        """
        Select entries from the store that match the given parameters.

        :param paramAndValues:
        :return:
        """

    @abstractmethod
    def lastCount(self):
        """
        Gives the last serial number in store.
        """
