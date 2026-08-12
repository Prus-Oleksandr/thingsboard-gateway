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

from threading import Thread
from random import choice
from string import ascii_lowercase
import asyncio

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import STATISTIC_MESSAGE_RECEIVED_PARAMETER, STATISTIC_MESSAGE_SENT_PARAMETER
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.tb_utility.tb_logger import init_logger


class S7Connector(Thread, Connector):
    def __init__(self, gateway, config, connector_type) -> None:
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        self.__connector_type = connector_type
        super().__init__()
        self.__gateway: TBGatewayService = gateway
        self.__config = config
        self.name = config.get('name', 'S7 ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        remote_logging = self.__config.get('enableRemoteLogging', False)
        log_level = self.__config.get('logLevel', "INFO")

        self.__log = init_logger(self.__gateway, self.name, log_level,
                                 enable_remote_logging=remote_logging,
                                 is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, self.name + '_converter', log_level,
                                           enable_remote_logging=remote_logging,
                                           is_converter_logger=True, attr_name=self.name)
        self.__log.info('Starting S7 connector...')

        self.__id = self.__config.get('id')
        self.daemon = True
        self.__stopped = False
        self.__connected = False

        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

    def open(self):
        self.start()

    def run(self):
        self.__connected = True

    def close(self):
        self.__log.info('Stopping BACnet connector...')
        self.__connected = False
        self.__stopped = True
        
        self.__log.info('BACnet connector stopped')
        self.__log.stop()

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self.__connector_type

    @property
    def connector_type(self):
        return self.__connector_type

    def get_config(self):
        return self.__config

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped
