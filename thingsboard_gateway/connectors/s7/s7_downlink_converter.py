#     Copyright 2026. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import struct
import snap7.util

from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class S7DownlinkConverter:
    def __init__(self, logger):
        self._log = logger

    def convert(self, config, data):
        request_type = config.get('type')
        if request_type == 'data':
            return self._convert_data_request(config, data)
        elif request_type == 'tag':
            # TODO: Implement tag request conversion
            pass
        elif request_type == 'vm':
            # TODO: Implement VM request conversion
            pass
        else:
            self._log.error(
                f"Unsupported request type '{request_type}' for downlink conversion.")
            return None

    def _convert_data_request(self, config, data):
        data_type = str(config.get("dataType", "raw")).lower().strip()
        offset = config.get("offset", 0)
        bit_index = config.get("bit", config.get("bitIndex", 0))
        specified_size = config.get("size", 0)

        type_sizes = {
            "bool": 1,
            "boolean": 1,
            "bit": 1,
            "byte": 1,
            "usint": 1,
            "uint8": 1,
            "sint": 1,
            "int8": 1,
            "int": 2,
            "int16": 2,
            "short": 2,
            "uint": 2,
            "uint16": 2,
            "word": 2,
            "dint": 4,
            "int32": 4,
            "udint": 4,
            "uint32": 4,
            "dword": 4,
            "real": 4,
            "float": 4,
            "float32": 4,
            "lreal": 8,
            "double": 8,
            "float64": 8,
        }

        min_size = type_sizes.get(data_type, 0)

        if data_type in ("string", "str", "s7string"):
            max_len = config.get(
                "maxLength", specified_size - 2 if specified_size > 2 else 254)
            min_size = max_len + 2
        elif data_type in ("raw", "bytes", "bytearray", "array"):
            min_size = len(data)

        buf_size = max(specified_size, offset + min_size)
        buf = bytearray(buf_size)

        if data_type in ("bool", "boolean", "bit"):
            bool_value = TBUtility.str_to_bool(data)
            snap7.util.set_bool(buf, offset, bit_index, bool_value)

        elif data_type in ("byte", "usint", "uint8"):
            snap7.util.set_usint(buf, offset, int(data))

        elif data_type in ("sint", "int8"):
            struct.pack_into(">b", buf, offset, int(data))

        elif data_type in ("int", "int16", "short"):
            snap7.util.set_int(buf, offset, int(data))

        elif data_type in ("uint", "uint16", "word"):
            snap7.util.set_uint(buf, offset, int(data))

        elif data_type in ("dint", "int32"):
            snap7.util.set_dint(buf, offset, int(data))

        elif data_type in ("udint", "uint32", "dword"):
            snap7.util.set_dword(buf, offset, int(data))

        elif data_type in ("real", "float", "float32"):
            snap7.util.set_real(buf, offset, float(data))

        elif data_type in ("lreal", "double", "float64"):
            struct.pack_into(">d", buf, offset, float(data))

        elif data_type in ("string", "str", "s7string"):
            str_val = str(data)
            max_len = config.get(
                "maxLength", specified_size - 2 if specified_size > 2 else 254)
            snap7.util.set_string(buf, offset, str_val, max_len)

        elif data_type in ("raw", "bytes", "bytearray", "array"):
            raw_bytes = bytearray(data)
            buf[offset: offset + len(raw_bytes)] = raw_bytes

        else:
            raise ValueError(f"Unsupported dataType: '{data_type}'")

        return buf
