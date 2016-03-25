import base64
from collections import OrderedDict
from copy import deepcopy
from json.decoder import WHITESPACE
from typing import Dict
import json

from ledger.immutable_store.test.mappingserializer import MappingSerializer


# Consider using bson or ubjson for serializing json


class OrderedJsonEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        kwargs["ensure_ascii"] = False
        super().__init__(*args, *kwargs)

    def encode(self, o):
        if isinstance(o, bytes) or isinstance(o, bytearray):
            return '"{}"'.format(base64.b64encode(o).decode("utf-8"))
        if isinstance(o, OrderedDict):
            return "{" + ",".join([self.encode(k)+":"+self.encode(v)
                                   for (k, v) in o.items()]) + "}"
        else:
            return json.JSONEncoder.encode(self, o)


class Base64Serializer(MappingSerializer):
    jsonEncoder = OrderedJsonEncoder()

    def serialize(self, data: Dict, fields, toBytes=True):
        data = deepcopy(data)
        orderedData = OrderedDict()
        for f in fields:
            if f in data:
                orderedData[f] = data.pop(f)
        assert not data
        jsonData = self.jsonEncoder.encode(orderedData)
        encoded = base64.b64encode(jsonData.encode())
        if not toBytes:
            encoded = encoded.decode('utf-8')
        return encoded

    def deserialize(self, data):
        jsonData = base64.b64decode(data).decode('utf-8')
        return json.loads(jsonData)
