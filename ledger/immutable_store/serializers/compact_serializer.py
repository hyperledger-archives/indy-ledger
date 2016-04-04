import base64
from collections import OrderedDict
from typing import Dict

import binascii

from ledger.immutable_store.serializers import MappingSerializer


class CompactSerializer(MappingSerializer):

    def __init__(self, orderedFields: OrderedDict=None):
        self.orderedFields = orderedFields
        self.delimiter = "|"

    def stringify(self, name, record, orderedFields=None):
        orderedFields = orderedFields or self.orderedFields
        if record is None or record == {}:
            return ""
        encoder = orderedFields[name][0] or str
        return encoder(record)

    def destringify(self, name, string, orderedFields=None):
        if not string:
            return None
        orderedFields = orderedFields or self.orderedFields
        decoder = orderedFields[name][1] or str
        return decoder(string)

    def serialize(self, data: Dict, orderedFields=None, toBytes=True):
        orderedFields = orderedFields or self.orderedFields
        records = []

        def _addToRecords(name, record):
            records.append(self.stringify(name, record, orderedFields))

        for name in orderedFields:
            if "." in name:
                nameParts = name.split(".")
                record = data.get(nameParts[0], {})
                for part in nameParts[1:]:
                    record = record.get(part, {})
            else:
                record = data.get(name)
            _addToRecords(name, record)

        encoded = self.delimiter.join(records)
        if toBytes:
            encoded = encoded.encode()
        return encoded

    def deserialize(self, data, orderedFields=None):
        orderedFields = orderedFields or self.orderedFields
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()
        data = data.split(self.delimiter)
        result = {}
        for name in orderedFields:
            if "." in name:
                nameParts = name.split(".")
                ref = result
                for part in nameParts[:-1]:
                    if part not in ref:
                        ref[part] = {}
                    ref = ref[part]
                ref[nameParts[-1]] = self.destringify(name, data.pop(0), orderedFields)
            else:
                result[name] = self.destringify(name, data.pop(0), orderedFields)
        return result
