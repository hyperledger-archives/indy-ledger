class MappingSerializer:
    def serialize(self, data, fields, toBytes=False):
        raise NotImplementedError

    def deserialize(self, data):
        raise NotImplementedError
