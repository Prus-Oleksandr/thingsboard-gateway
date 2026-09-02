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

from ipaddress import IPv4Address, AddressValueError

from puresnmp.types import Integer, OctetString, IpAddress, Counter, Gauge, Counter64, TimeTicks

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics


class SNMPDownlinkConverter(Converter):
    _INT_HANDLER = lambda v: Integer(int(v))
    _COUNTER_HANDLER = lambda v: Counter(int(v))
    _COUNTER64_HANDLER = lambda v: Counter64(int(v))
    _GAUGE_HANDLER = lambda v: Gauge(int(v))
    _TIMETICKS_HANDLER = lambda v: TimeTicks(int(v))
    _OCTETSTRING_HANDLER = lambda v: OctetString(v.encode() if isinstance(v, str) else v)
    _IPADDRESS_HANDLER = lambda v: IpAddress(IPv4Address(v))

    _TYPE_HANDLERS = {
        'INTEGER': _INT_HANDLER,
        'INT': _INT_HANDLER,
        'COUNTER': _COUNTER_HANDLER,
        'COUNTER32': _COUNTER_HANDLER,
        'COUNTER64': _COUNTER64_HANDLER,
        'GAUGE': _GAUGE_HANDLER,
        'GAUGE32': _GAUGE_HANDLER,
        'TIMETICKS': _TIMETICKS_HANDLER,
        'OCTETSTRING': _OCTETSTRING_HANDLER,
        'STRING': _OCTETSTRING_HANDLER,
        'STR': _OCTETSTRING_HANDLER,
        'IPADDRESS': _IPADDRESS_HANDLER,
    }

    def __init__(self, config):
        self.__config = config

    @CollectStatistics(start_stat_type='allReceivedBytesFromTB',
                       end_stat_type='allBytesSentToDevices')
    def convert(self, config, data):
        value = data.get("params")
        if value is None:
            return Integer(0)

        snmp_type = config.get("type", "OCTETSTRING").upper()
        handler = self._TYPE_HANDLERS.get(snmp_type)

        if handler:
            try:
                return handler(value)
            except (ValueError, TypeError, AddressValueError):
                raise ValueError(f"Cannot convert value '{value}' to type '{snmp_type}'")

        raise ValueError(f"Unsupported SNMP type: {snmp_type}")
