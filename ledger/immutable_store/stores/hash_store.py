from abc import abstractmethod


class HashStore:
    # TODO: Probably method names should write/read or put/get. write/get
    # seems odd
    @abstractmethod
    def writeLeaf(self, leaf):
        pass

    @abstractmethod
    def writeNode(self, node):
        pass

    @abstractmethod
    def getLeaf(self, pos):
        pass

    @abstractmethod
    def getNode(self, pos):
        pass
