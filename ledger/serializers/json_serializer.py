import base64
import ujson as json
from ujson import encode as uencode
from typing import Dict

from ledger.serializers.mapping_serializer import MappingSerializer


# Consider using bson or ubjson for serializing json


# class OrderedJsonEncoder(json.JSONEncoder):
#     def __init__(self, *args, **kwargs):
#         kwargs['ensure_ascii'] = False
#         kwargs['sort_keys'] = True
#         kwargs['separators'] = (',', ':')
#         super().__init__(*args, **kwargs)
#
#     def encode(self, o):
#         if isinstance(o, (bytes, bytearray)):
#             return '"{}"'.format(base64.b64encode(o).decode("utf-8"))
#         else:
#             return json.JSONEncoder.encode(self, o)


class JsonSerializer(MappingSerializer):
    """
    Class to convert a mapping to json with keys ordered in lexicographical
    order
    """
    # jsonEncoder = OrderedJsonEncoder()

    @staticmethod
    def _encode(o):
        if isinstance(o, (bytes, bytearray)):
            return '"{}"'.format(base64.b64encode(o).decode("utf-8"))
        else:
            return uencode(o, sort_keys=True)

    # The `fields` argument is kept to conform to the interface, its not
    # need in this method
    def serialize(self, data: Dict, fields=None, toBytes=True):
        # encoded = self.jsonEncoder.encode(data)
        encoded = self._encode(data)
        if toBytes:
            encoded = encoded.encode()
        return encoded

    # The `fields` argument is kept to conform to the interface, its not
    # need in this method
    def deserialize(self, data, fields=None):
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()
        return json.loads(data)
