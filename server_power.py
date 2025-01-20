import asyncio
import logging
import pprint
import traceback

from asyncua import Node, Server, ua
from asyncua.ua import NodeId
from pyemvue.device import VueDeviceChannelUsage, VueUsageDevice

from emporia import EmporiaVueClient
from helpers import bcolors

_logger = logging.getLogger(__name__)

EMPORIA_PORT = 4841


class AsyncServer:

    uri: str
    index: int
    vue: EmporiaVueClient
    device_nodes: dict = {}

    def __init__(self, endpoint, name):
        self.server = Server()
        self.server.set_server_name(name)
        self.server.set_endpoint(endpoint)

        self.vue = EmporiaVueClient()

    async def init(self):

        await self.server.init()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        await self.server.set_application_uri("urn:trinova.com:Trinova:opcua")

        self.uri = "http://examples.freeopcua.github.io"
        self.index = await self.server.register_namespace(self.uri)

        print(bcolors.WARNING + "WAITING " + bcolors.ENDC + "Logging in to Emporia...")

        # Build out the Vue device nodes
        self.vue.login()

        print(bcolors.OKGREEN + "SUCCESS " + bcolors.ENDC + "Logged in to Emporia...")

        await self.build_emporia_objects()

        print(bcolors.OKBLUE + "DONE " + bcolors.ENDC + "Node IDs created...")

    async def build_emporia_objects(self):

        # Create a parent folder to hold devices
        emporia_devices_folder = await self.server.nodes.objects.add_folder(
            self.index,
            "Vue Devices",
        )

        self.vue.get_devices()

        for device in self.vue.device_info:

            device_q_name = device.display_name.replace(" ", "_")

            if device.device_name:

                # Initialize node object
                self.device_nodes[device.device_gid] = {
                    "node": await emporia_devices_folder.add_object(
                        self.index,
                        device.device_name,
                    ),
                    "channels": {},
                }

                device_usage: VueUsageDevice
                device_usage = self.vue.device_usage_dict[device.device_gid]

                # Add Channels object to node
                self.device_nodes[device.device_gid]["channels"][
                    "node"
                ] = await self.device_nodes[device.device_gid]["node"].add_object(
                    self.index,
                    "Channels",
                )

                for key, param in device_usage.__dict__.items():

                    try:

                        if str(key) == "channels":

                            channel_usage: VueDeviceChannelUsage
                            for channel_number, channel_usage in param.items():

                                # print("Build...")
                                # pprint.pp(channel_usage.__dict__)

                                channel_q_name = (
                                    (f"{ device_q_name }.channel_{ channel_number }")
                                    .replace(" ", "_")
                                    .replace(",", "_")
                                )

                                # Initialize Channel object
                                self.device_nodes[device.device_gid]["channels"][
                                    str(channel_number)
                                ] = {}

                                # Add Channel node
                                self.device_nodes[device.device_gid]["channels"][
                                    str(channel_number)
                                ]["node"] = await self.device_nodes[device.device_gid][
                                    "channels"
                                ][
                                    "node"
                                ].add_object(
                                    self.index,
                                    f"Channel { channel_number }",
                                )

                                # Add Channel variables to node
                                channel_node: dict[str, Node]
                                channel_node = self.device_nodes[device.device_gid][
                                    "channels"
                                ][str(channel_number)]

                                # Calculate kWh & Wh
                                if channel_usage.usage is not None:
                                    _wh = ua.DataValue(
                                        Value=float(channel_usage.usage * 1000),
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                                    )
                                    _kwh = ua.DataValue(
                                        Value=float(channel_usage.usage),
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                                    )
                                    _percentage = ua.DataValue(
                                        Value=float(channel_usage.percentage),
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                                    )
                                else:
                                    _wh = ua.DataValue(
                                        Value=0.0,
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                                    )
                                    _kwh = ua.DataValue(
                                        Value=0.0,
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                                    )
                                    _percentage = ua.DataValue(
                                        Value=0.0,
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                                    )

                                channel_node["usage_kwh"] = await channel_node[
                                    "node"
                                ].add_variable(
                                    nodeid=NodeId(
                                        Identifier=f"{ channel_q_name }.usage_kwh",
                                        NamespaceIndex=self.index,
                                    ),
                                    bname="usage_kwh",
                                    val=_kwh,
                                    varianttype=ua.VariantType.Double,
                                )

                                channel_node["usage_wh"] = await channel_node[
                                    "node"
                                ].add_variable(
                                    nodeid=NodeId(
                                        Identifier=f"{ channel_q_name }.usage_wh",
                                        NamespaceIndex=self.index,
                                    ),
                                    bname="usage_wh",
                                    val=_wh,
                                    varianttype=ua.VariantType.Double,
                                )
                                channel_node["percentage"] = await channel_node[
                                    "node"
                                ].add_variable(
                                    nodeid=NodeId(
                                        Identifier=f"{ channel_q_name }.percentage",
                                        NamespaceIndex=self.index,
                                    ),
                                    bname="percentage",
                                    val=_percentage,
                                    varianttype=ua.VariantType.Double,
                                )
                                channel_node["channel_num"] = await channel_node[
                                    "node"
                                ].add_variable(
                                    nodeid=NodeId(
                                        Identifier=f"{ channel_q_name }.channel_num",
                                        NamespaceIndex=self.index,
                                    ),
                                    bname="channel_num",
                                    val=channel_usage.channel_num,
                                    varianttype=ua.VariantType.String,
                                )
                                channel_node["name"] = await channel_node[
                                    "node"
                                ].add_variable(
                                    nodeid=NodeId(
                                        Identifier=f"{ channel_q_name }.name",
                                        NamespaceIndex=self.index,
                                    ),
                                    bname="name",
                                    val=channel_usage.name,
                                    varianttype=ua.VariantType.String,
                                )

                                print(
                                    bcolors.OKGREEN
                                    + "SUCCESS "
                                    + bcolors.ENDC
                                    + f"Channel node object created: { channel_q_name }"
                                )
                        else:
                            # Add other device parameters to Device node
                            self.device_nodes[device.device_gid][
                                key
                            ] = await self.device_nodes[device.device_gid][
                                "node"
                            ].add_variable(
                                nodeid=NodeId(
                                    Identifier=f"{ device_q_name }.{ key }",
                                    NamespaceIndex=self.index,
                                ),
                                bname=key,
                                val=param,
                            )

                            print(
                                bcolors.OKGREEN
                                + "SUCCESS "
                                + bcolors.ENDC
                                + f"Node ID created: { device_q_name }.{ key }"
                            )

                    except Exception as e:

                        print(
                            bcolors.FAIL
                            + "FAILED "
                            + bcolors.ENDC
                            + f"Failed to create Node ID: { channel_q_name }"
                        )

                        traceback.print_exc()

    async def update_emporia_device_usage(self):

        try:

            # Update device usage deict
            await asyncio.to_thread(self.vue.get_device_usage)

            for device_gid, device_node in self.device_nodes.items():

                if device_gid not in self.vue.device_usage_dict:

                    print(
                        bcolors.WARNING
                        + "WARNING "
                        + bcolors.ENDC
                        + f"Device GID not found in usage dict: { device_gid }"
                    )
                    continue

                device_usage: VueUsageDevice
                device_usage = self.vue.device_usage_dict[device_gid]

                # pprint.pp(device_usage.__dict__)

                for key, param in device_usage.__dict__.items():

                    if str(key) == "channels":

                        channel_usage: VueDeviceChannelUsage
                        for channel_number, channel_usage in param.items():

                            try:

                                # print("Update...")
                                # pprint.pp(channel_usage.__dict__)

                                # print(channel_usage.__dict__)

                                channel_node = device_node["channels"][
                                    str(channel_number)
                                ]

                                # Calculate kWh & Wh
                                if channel_usage.usage is not None:
                                    _wh = ua.DataValue(
                                        Value=float(channel_usage.usage * 1000),
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                                    )
                                    _kwh = ua.DataValue(
                                        Value=float(channel_usage.usage),
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                                    )
                                    _percentage = ua.DataValue(
                                        Value=float(channel_usage.percentage),
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                                    )
                                else:
                                    print(
                                        bcolors.WARNING
                                        + "WARNING "
                                        + bcolors.ENDC
                                        + f"Channel usage null for { str(channel_number) }"
                                    )
                                    _wh = ua.DataValue(
                                        Value=0.0,
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                                    )
                                    _kwh = ua.DataValue(
                                        Value=0.0,
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                                    )
                                    _percentage = ua.DataValue(
                                        Value=0.0,
                                        StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                                    )

                                await channel_node["usage_kwh"].set_value(_kwh)
                                await channel_node["usage_wh"].set_value(_wh)
                                await channel_node["percentage"].set_value(_percentage)
                                await channel_node["channel_num"].set_value(
                                    channel_usage.channel_num
                                )
                                await channel_node["name"].set_value(channel_usage.name)

                                print(
                                    bcolors.OKGREEN
                                    + "SUCCESS "
                                    + bcolors.ENDC
                                    + f"Channel values updated for { channel_number }"
                                )

                            except Exception as e:

                                print(
                                    bcolors.FAIL
                                    + "FAILED "
                                    + bcolors.ENDC
                                    + f"Set channel values on { channel_number }"
                                )

                                traceback.print_exc()

                    else:

                        try:

                            await device_node[key].set_value(param)

                            print(
                                bcolors.OKGREEN
                                + "SUCCESS "
                                + bcolors.ENDC
                                + f"Set { device_node[key].nodeid.Identifier } = { param }"
                            )

                        except Exception as e:

                            # pprint.pp(device_node[key])
                            # print(key)
                            # pprint.pp(device_node[key])

                            print(
                                bcolors.FAIL
                                + "FAILED "
                                + bcolors.ENDC
                                + f"Set { device_node[key] } = { param }"
                            )

                            traceback.print_exc()

        except Exception as e:

            traceback.print_exc()

    async def __aenter__(self):
        await self.init()
        await self.server.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.server.stop()


async def periodic_update(client: AsyncServer):
    while True:

        await client.update_emporia_device_usage()  # Fetch the usage data periodically

        print(bcolors.OKBLUE + "DONE " + bcolors.ENDC + "Node values updated...")

        await asyncio.sleep(10)  # Adjust sleep time based on your requirements


async def main():
    async with AsyncServer(
        f"opc.tcp://0.0.0.0:{ EMPORIA_PORT }",
        "TriNova AC Project - Emporia",
    ) as client:

        print(bcolors.OKBLUE + "DONE " + bcolors.ENDC + "Service starting...")

        # Start the update_device_usage function as a background task
        asyncio.create_task(periodic_update(client))

        while True:

            await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
