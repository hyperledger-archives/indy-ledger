class MappingSerializer:
    def serialize(self, data, fields=None, toBytes=False):
        raise NotImplementedError

    def deserialize(self, data):
        raise NotImplementedError
