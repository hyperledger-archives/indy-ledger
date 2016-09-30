from abc import abstractmethod, abstractproperty

from ledger.util import count_bits_set
from ledger.util import highest_bit_set


class HashStore:
    """
    Store of nodeHashes and leafHashes mapped against their sequence numbers.
    """

    @abstractmethod
    def writeLeaf(self, leafHash):
        """
        append the leafHash to the leaf hash store

        :param leafHash: hash of the leaf
        """
        pass

    @abstractmethod
    def writeNode(self, node):
        """
        append the node to the node hash store.

        :param node: tuple of start, height and nodeHash
        """
        pass

    @abstractmethod
    def readLeaf(self, pos):
        """
        Read the leaf hash at the given position in the merkle tree

        :param pos: the sequence number of the leaf
        :return: the leafHash at the specified position
        """
        pass

    @abstractmethod
    def readNode(self, pos):
        """
        Read the node hash at the given position in the merkle tree

        :param pos: the sequence number of the node (as calculated by
        getNodePosition)
        :return: the nodeHash at the specified position
        """
        pass

    @abstractmethod
    def readLeafs(self, startpos, endpos):
        """
        Read multiple leaves at the given position.

        :param startpos: read from this sequence number (inclusive)
        :param endpos: read up to this sequence number (inclusive)
        :return: list of leafHashes
        """
        pass

    @abstractmethod
    def readNodes(self, startpos, endpos):
        """
        Read multiple nodes at the given position. Node position can be
         calculated using getNodePosition

        :param startpos: read from this sequence number (inclusive)
        :param endpos: read up to this sequence number (inclusive)
        :return: list of nodeHashes
        """
        pass

    @abstractproperty
    def leafCount(self) -> int:
        pass

    @leafCount.setter
    @abstractmethod
    def leafCount(self, count: int) -> None:
        pass

    @abstractproperty
    def nodeCount(self) -> None:
        pass

    @classmethod
    def getNodePosition(cls, start, height=None) -> int:
        """
        Calculates node position based on start and height

        :param start: The sequence number of the first leaf under this tree.
        :param height: Height of this node in the merkle tree
        :return: the node's position
        """
        pwr = highest_bit_set(start) - 1
        height = height or pwr
        if count_bits_set(start) == 1:
            adj = height - pwr
            return start - 1 + adj
        else:
            c = pow(2, pwr)
            return cls.getNodePosition(c, pwr) + \
                   cls.getNodePosition(start - c, height)

    @classmethod
    def getPath(cls, seqNo, offset=0):
        """
        Get the audit path of the leaf at the position specified by serNo.

        :param seqNo: sequence number of the leaf to calculate the path for
        :param offset: the sequence number of the node from where the path
         should begin.
        :return: tuple of leafs and nodes
        """
        if offset >= seqNo:
            raise ValueError("Offset should be less than serial number")
        pwr = highest_bit_set(seqNo - 1 - offset) - 1
        if pwr <= 0:
            if seqNo % 2 == 0:
                return [seqNo - 1], []
            else:
                return [], []
        c = pow(2, pwr) + offset
        leafs, nodes = cls.getPath(seqNo, c)
        nodes.append(cls.getNodePosition(c, pwr))
        return leafs, nodes

    def readNodeByTree(self, start, height=None):
        """
        Fetches nodeHash based on start leaf and height of the node in the tree.

        :return: the nodeHash
        """
        pos = self.getNodePosition(start, height)
        return self.readNode(pos)

    @abstractmethod
    def reset(self) -> bool:
        """
        Removes all data from hash store

        :return: True if completed successfully
        """