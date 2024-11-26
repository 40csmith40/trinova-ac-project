import logging
import os

import pyemvue
from pyemvue.device import VueDevice, VueUsageDevice
from pyemvue.enums import Scale, Unit

_logger = logging.getLogger(__name__)


class EmporiaVueClient:

    client: pyemvue.PyEmVue
    device_info: list[VueDevice]
    device_usage_dict: dict[int, VueUsageDevice]
    device_gids: list[int]

    def __init__(self) -> None:

        self.client = pyemvue.PyEmVue()

    def login(self):

        print("Logging in to Emporia...")

        self.client.login(
            username=os.getenv("EMPORIA_USERNAME"),
            password=os.getenv("EMPORIA_PASSWORD"),
            token_storage_file="keys.json",
        )

        print("Emporia login complete...")

    def get_devices(self) -> None:
        """
        Get device info & usage
        """

        print("Gathering Emporia device info...")

        self.device_info = self.client.get_devices()

        # Make list of device GIDs
        self.device_gids = list({device.device_gid for device in self.device_info})

        self.get_device_usage()

    def get_device_usage(self):

        print("Gathering Emporia device usage...")

        # Get device usage objects from GID list
        self.device_usage_dict = self.client.get_device_list_usage(
            deviceGids=self.device_gids,
            instant=None,
            scale=Scale.MINUTE.value,
            unit=Unit.KWH.value,
        )
