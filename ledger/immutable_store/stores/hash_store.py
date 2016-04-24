from abc import abstractmethod

from ledger.immutable_store.util import count_bits_set
from ledger.immutable_store.util import highest_bit_set


class HashStore:
    @abstractmethod
    def writeLeaf(self, leafHash):
        pass

    @abstractmethod
    def writeNode(self, nodeHash):
        pass

    @abstractmethod
    def readLeaf(self, pos):
        pass

    @abstractmethod
    def readNode(self, pos):
        pass

    @abstractmethod
    def readLeafs(self, startpos, endpos):
        pass

    @abstractmethod
    def readNodes(self, startpos, endpos):
        pass

    @classmethod
    def getNodePosition(cls, start, height=None):
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
    def getPath(cls, serNo, offset=0):
        if offset >= serNo:
            raise ValueError("Offset should be less than serial number")
        pwr = highest_bit_set(serNo-1-offset) - 1
        if pwr <= 0:
            if serNo % 2 == 0:
                return [serNo - 1], []
            else:
                return [], []
        c = pow(2, pwr) + offset
        leafs, nodes = cls.getPath(serNo, c)
        nodes.append(cls.getNodePosition(c, pwr))
        return leafs, nodes

    def readNodeByTree(self, start, height=None):
        pos = self.getNodePosition(start, height)
        return self.readNode(pos)
