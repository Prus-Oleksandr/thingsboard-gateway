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

from thingsboard_gateway.connectors.s7.entities.device import Device
from thingsboard_gateway.connectors.s7.entities.device_configs import (
    DeviceConfigValidationError,
)
from time import monotonic, sleep
import asyncio
from random import choice
from string import ascii_lowercase
from threading import Thread
from packaging import version

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import (
    STATISTIC_MESSAGE_RECEIVED_PARAMETER,
    STATISTIC_MESSAGE_SENT_PARAMETER,
)
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService

installation_required = False
required_version = '3.1.2'
force_install = False

try:
    from snap7 import __version__ as s7_version

    if version.parse(s7_version) != version.parse(required_version):
        installation_required = True
        force_install = True
except ImportError:
    installation_required = True

if installation_required:
    print('S7 library not found - installing...')
    TBUtility.install_package(
        'python-snap7', required_version, force_install=force_install
    )


class S7Connector(Thread, Connector):
    def __init__(self, gateway, config, connector_type) -> None:
        self.statistics = {
            STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
            STATISTIC_MESSAGE_SENT_PARAMETER: 0,
        }
        self.__connector_type = connector_type
        super().__init__()
        self.__gateway: TBGatewayService = gateway
        self.__config = config
        self.name = config.get(
            'name', 'S7 ' + ''.join(choice(ascii_lowercase) for _ in range(5))
        )
        remote_logging = self.__config.get('enableRemoteLogging', False)
        log_level = self.__config.get('logLevel', 'INFO')

        self.__log = init_logger(
            self.__gateway,
            self.name,
            log_level,
            enable_remote_logging=remote_logging,
            is_connector_logger=True,
        )
        self.__converter_log = init_logger(
            self.__gateway,
            self.name + '_converter',
            log_level,
            enable_remote_logging=remote_logging,
            is_converter_logger=True,
            attr_name=self.name,
        )
        self.__log.info('Starting S7 connector...')

        self.__id = self.__config.get('id')
        self.daemon = True
        self.__stopped = False
        self.__connected = False

        self.__process_device_queue = asyncio.Queue(1_000_000)
        self.__data_to_convert_queue = asyncio.Queue(1_000_000)
        self.__data_to_save_queue = asyncio.Queue(1_000_000)

        self._devices = []

        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        self.loop.set_exception_handler(self.exception_handler)

    def exception_handler(self, _, context):
        if context.get('exception') is not None:
            self.__log.exception('handled exception',
                                 exc_info=context['exception'])

    def open(self):
        self.start()

    def run(self):
        self.__connected = True

        try:
            self.loop.run_until_complete(self._run())
        except asyncio.CancelledError as e:
            self.__log.debug(
                'Task was cancelled due to connector stop: %s', e.__str__()
            )
        except Exception as e:
            self.__log.exception(e)

    async def _run(self):
        await self._load_devices()
        await self._connect_to_devices()

        await asyncio.gather(
            self._run_devices(),
            self._read_data_from_devices(),
            self._convert_data(),
            self._save_data(),
        )

    async def _load_devices(self) -> None:
        if len(self.__config.get('devices', [])) == 0:
            self.__log.error('Device list is empty.')
            return

        for device_config in self.__config['devices']:
            try:
                device = Device.create_device_from_config(
                    self.__log, self.__converter_log, device_config, self.__process_device_queue)
                self._devices.append(device)
            except DeviceConfigValidationError as e:
                self.__log.error(
                    'Error creating %s device: %s', device_config, e)

    async def _connect_to_devices(self) -> None:
        tasks = [device.connect() for device in self._devices]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self.__log.error('Error connecting to device: %s', result)

    async def _run_devices(self) -> None:
        tasks = [device.run() for device in self._devices]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _read_data_from_devices(self) -> None:
        while not self.__stopped:
            try:
                device: Device = self.__process_device_queue.get_nowait()
                if device.stopped:
                    self.__log.trace(
                        'Device %s is stopped, skipping read.', device.config.device_name)
                    continue

                results = await device.read_configured_data()
                if len(results) <= 0:
                    self.__log.trace(
                        'No data read from device %s.', device.config.device_name)
                    continue

                self.__data_to_convert_queue.put_nowait((device, results))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.exception('Error processing device request: %s', e)

    async def _convert_data(self):
        while not self.__stopped:
            try:
                device, values = self.__data_to_convert_queue.get_nowait()
                self.__log.trace('%s data to convert: %s',
                                 device.config.device_name, values)

                converted_data = device.uplink_converter.convert(values)
                self.__data_to_save_queue.put_nowait((device, converted_data))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.exception('Error converting data: %s', e)

    async def _save_data(self):
        while not self.__stopped:
            try:
                device, data_to_save = self.__data_to_save_queue.get_nowait()
                self.__log.trace('%s data to save: %s',
                                 device.config.device_name, data_to_save)
                StatisticsService.count_connector_message(
                    self.get_name(), stat_parameter_name='storageMsgPushed')
                self.__gateway.send_to_storage(
                    self.get_name(), self.get_id(), data_to_save)
                self.statistics[STATISTIC_MESSAGE_SENT_PARAMETER] += 1
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.exception('Error saving data: %s', e)

    def close(self):
        self.__log.info('Stopping S7 connector...')
        self.__connected = False
        self.__stopped = True

        self._stop_devices()

        asyncio.run_coroutine_threadsafe(self.__cancel_all_tasks(), self.loop)

        self.__check_is_alive()

        self.__log.info('S7 connector stopped')
        self.__log.stop()

    def _stop_devices(self):
        for device in self._devices:
            device.stop()

    def __check_is_alive(self):
        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 10:
                self.__log.error(
                    "Failed to stop connector %s", self.get_name())
                break
            sleep(.1)

    async def __cancel_all_tasks(self):
        await asyncio.sleep(5)
        for task in asyncio.all_tasks(self.loop):
            task.cancel()

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
