import asyncio
import logging
import os
import pprint

import pyemvue
from asyncua import Server, ua
from pyemvue.device import VueDevice, VueDeviceChannelUsage, VueUsageDevice
from pyemvue.enums import Scale, Unit

_logger = logging.getLogger(__name__)


async def main():

    server = Server()
    await server.init()
    server.set_server_name("FreeOpcUa Example Server")

    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    await server.set_application_uri("urn:trinova.com:Trinova:opcua")
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")

    # setup our own namespace
    uri = "http://examples.freeopcua.github.io"
    idx = await server.register_namespace(uri)

    # Login to Vue
    vue = pyemvue.PyEmVue()
    vue.login(
        username=os.getenv("EMPORIA_USERNAME"),
        password=os.getenv("EMPORIA_PASSWORD"),
        token_storage_file="keys.json",
    )

    # Get device info
    devices = vue.get_devices()

    # Make list of device GIDs
    device_gids = list({device.device_gid for device in devices})

    # Get device usage objects from GID list
    device_usage_dict = vue.get_device_list_usage(
        deviceGids=device_gids,
        instant=None,
        scale=Scale.MINUTE.value,
        unit=Unit.KWH.value,
    )

    # Create a parent folder to hold devices
    devices_folder = await server.nodes.objects.add_folder(idx, "Devices")

    device_nodes = {}

    # Add device info to OPC-UA
    device: VueDevice
    for device in devices:

        if device.device_name:

            # Initialize node object
            device_nodes[device.device_gid] = {}

            # Save node for future browsing
            device_nodes[device.device_gid]["node"] = await devices_folder.add_object(
                idx,
                device.device_name,
            )

            device_usage: VueUsageDevice
            device_usage = device_usage_dict[device.device_gid]

            device_nodes[device.device_gid]["channels"] = {}

            # Add Channels object to node
            device_nodes[device.device_gid]["channels"]["node"] = await device_nodes[
                device.device_gid
            ]["node"].add_object(
                idx,
                "Channels",
            )

            for key, param in device_usage.__dict__.items():
                if str(key) == "channels":

                    channel_usage: VueDeviceChannelUsage
                    for channel_number, channel_usage in param.items():

                        # Initialize Channel object
                        device_nodes[device.device_gid]["channels"][
                            str(channel_number)
                        ] = {}

                        # Add Channel node
                        device_nodes[device.device_gid]["channels"][
                            str(channel_number)
                        ]["node"] = await device_nodes[device.device_gid]["channels"][
                            "node"
                        ].add_object(
                            idx,
                            f"Channel { channel_number }",
                        )

                        # Add Channel variables to node
                        channel_node = device_nodes[device.device_gid]["channels"][
                            str(channel_number)
                        ]
                        channel_node["usage kWh"] = await channel_node[
                            "node"
                        ].add_variable(
                            idx,
                            "usage kWh",
                            channel_usage.usage,
                        )
                        channel_node["usage Wh"] = await channel_node[
                            "node"
                        ].add_variable(
                            idx,
                            "usage Wh",
                            channel_usage.usage * 1000,
                        )
                        channel_node["percentage"] = await channel_node[
                            "node"
                        ].add_variable(
                            idx,
                            "percentage",
                            channel_usage.percentage,
                        )
                        channel_node["channel_num"] = await channel_node[
                            "node"
                        ].add_variable(
                            idx,
                            "channel_num",
                            channel_usage.channel_num,
                        )
                        channel_node["name"] = await channel_node["node"].add_variable(
                            idx,
                            "name",
                            channel_usage.name,
                        )
                else:
                    # Add other device parameters to Device node
                    device_nodes[device.device_gid][key] = await device_nodes[
                        device.device_gid
                    ]["node"].add_variable(idx, key, param)

    # pprint.pp(device_nodes)

    async with server:

        while True:

            await asyncio.sleep(5)

            # Get device usage objects from GID list
            device_usage_dict = vue.get_device_list_usage(
                deviceGids=device_gids,
                instant=None,
                scale=Scale.MINUTE.value,
                unit=Unit.KWH.value,
            )

            for device_gid, device_node in device_nodes.items():

                device_usage: VueUsageDevice
                device_usage = device_usage_dict[device_gid]

                for key, param in device_usage.__dict__.items():
                    if str(key) == "channels":

                        channel_usage: VueDeviceChannelUsage
                        for channel_number, channel_usage in param.items():

                            channel_node = device_node["channels"][str(channel_number)]

                            await channel_node["usage kWh"].set_value(
                                channel_usage.usage
                            )
                            await channel_node["usage Wh"].set_value(
                                channel_usage.usage * 1000
                            )
                            await channel_node["percentage"].set_value(
                                channel_usage.percentage
                            )
                            await channel_node["channel_num"].set_value(
                                channel_usage.channel_num
                            )
                            await channel_node["name"].set_value(channel_usage.name)

                    else:

                        await device_node[key].set_value(param)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    asyncio.run(main())
