import asyncio
import logging
import pprint
import traceback

from asyncua import Node, Server, ua
from asyncua.ua import NodeId
from pyemvue.device import VueDeviceChannelUsage, VueUsageDevice

from emporia import EmporiaVueClient
from hubitat import HubitatClient

_logger = logging.getLogger(__name__)

ADD_EMPORIA_DEVICES = True
ADD_HUBITAT_DEVICES = True


class AsyncServer:

    uri: str
    index: int
    vue: EmporiaVueClient
    hubitat: HubitatClient
    device_nodes: dict = {}

    def __init__(self, endpoint, name):
        self.server = Server()
        self.server.set_server_name(name)
        self.server.set_endpoint(endpoint)

        self.vue = EmporiaVueClient()
        self.hubitat = HubitatClient()

    async def init(self):

        await self.server.init()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        await self.server.set_application_uri("urn:trinova.com:Trinova:opcua")

        self.uri = "http://examples.freeopcua.github.io"
        self.index = await self.server.register_namespace(self.uri)

        # Build out the Hubitat devices
        if ADD_HUBITAT_DEVICES:
            await self.build_hubitat_objects()

        # Build out the Vue device nodes
        if ADD_EMPORIA_DEVICES:
            self.vue.login()
            await self.build_emporia_objects()

    async def build_hubitat_objects(self):

        # Create a parent folder to hold devices
        hubitat_devices_folder = await self.server.nodes.objects.add_folder(
            self.index,
            "Hubitat Devices",
        )

        self.hubitat.get_devices()

        for device in self.hubitat.devices:

            if "id" in device:
                _device_id = device["id"]
            else:
                continue

            if "name" in device:
                _device_name = device["name"]
            else:
                continue

            device_q_name = _device_name.replace(" ", "_")

            # Initialize node object
            self.device_nodes[_device_id] = {
                "node": await hubitat_devices_folder.add_object(
                    self.index,
                    _device_name,
                ),
                "type": "hubitat",
                "attributes": {},
            }

            device_info = self.hubitat.get_device_info(_device_id)

            if device_info:

                if "attributes" in device_info:
                    _device_attributes = device_info["attributes"]
                else:
                    continue

                for attribute in _device_attributes:

                    if "name" in attribute:
                        _attribute_name = attribute["name"]
                    else:
                        continue

                    if "currentValue" in attribute:
                        _attribute_value = attribute["currentValue"]
                    else:
                        continue

                    if "dataType" in attribute:
                        _attribute_type = attribute["dataType"]
                    else:
                        continue

                    if _attribute_value is None:
                        continue

                    if _attribute_type == "NUMBER":
                        _attribute_value = float(_attribute_value)
                        _attribute_varianttype = ua.VariantType.Double

                    elif _attribute_type == "STRING":
                        _attribute_value = str(_attribute_value)
                        _attribute_varianttype = ua.VariantType.String
                    else:
                        continue

                    try:

                        # Add other device parameters to Device node
                        self.device_nodes[_device_id]["attributes"][
                            _attribute_name
                        ] = await self.device_nodes[_device_id]["node"].add_variable(
                            nodeid=NodeId(
                                Identifier=f"{ device_q_name }.{ _attribute_name }",
                                NamespaceIndex=self.index,
                            ),
                            bname=_attribute_name,
                            val=_attribute_value,
                            varianttype=_attribute_varianttype,
                        )

                    except Exception as e:
                        traceback.print_exc()

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
                    "type": "emporia",
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
                    if str(key) == "channels":

                        channel_usage: VueDeviceChannelUsage
                        for channel_number, channel_usage in param.items():

                            print("Build...")
                            pprint.pp(channel_usage.__dict__)

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

        # print("Built...")
        # pprint.pp(self.device_nodes)

    async def update_emporia_device_usage(self):

        try:

            # Update device usage deict
            await asyncio.to_thread(self.vue.get_device_usage)

            # print("Updating...")
            # pprint.pp(self.device_nodes)

            for device_gid, device_node in self.device_nodes.items():

                if device_node["type"] != "emporia":
                    continue

                if device_gid not in self.vue.device_usage_dict:
                    continue

                device_usage: VueUsageDevice
                device_usage = self.vue.device_usage_dict[device_gid]

                for key, param in device_usage.__dict__.items():
                    if str(key) == "channels":

                        channel_usage: VueDeviceChannelUsage
                        for channel_number, channel_usage in param.items():

                            print("Update...")
                            pprint.pp(channel_usage.__dict__)

                            # print(channel_usage.__dict__)

                            channel_node = device_node["channels"][str(channel_number)]

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
                                print("NULLLLLLL -----------------")
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

                    else:

                        await device_node[key].set_value(param)

        except Exception as e:

            traceback.print_exc()

    async def update_hubitat_device_usage(self):

        try:

            # Update device usage deict
            # await asyncio.to_thread(self.hubitat.get_device_info)

            for device_id, device_node in self.device_nodes.items():

                if device_node["type"] != "hubitat":
                    continue

                device_info = self.hubitat.get_device_info(device_id)

                if "attributes" not in device_info:
                    print("Attribute not found")
                    continue

                _live_device_attributes = device_info["attributes"]

                for attribute in _live_device_attributes:

                    if "name" in attribute:
                        _attribute_name = attribute["name"]
                    else:
                        continue

                    if "currentValue" in attribute:
                        _attribute_value = attribute["currentValue"]
                    else:
                        continue

                    if "dataType" in attribute:
                        _attribute_type = attribute["dataType"]
                    else:
                        continue

                    if _attribute_name in device_node["attributes"]:

                        print(
                            f"Setting: { device_node['attributes'][_attribute_name] } = { _attribute_value }"
                        )

                        await device_node["attributes"][_attribute_name].set_value(
                            _attribute_value
                        )

                # for key, attribute in device_node["attributes"].items():

                #     if key in _live_device_attributes:

                # print(device_node)

                # device_usage: VueUsageDevice
                # device_usage = self.vue.device_usage_dict[device_gid]

                # for key, param in device_usage.__dict__.items():
                #     if str(key) == "channels":

                #         channel_usage: VueDeviceChannelUsage
                #         for channel_number, channel_usage in param.items():

                #             print(channel_usage.__dict__)

                #             channel_node = device_node["channels"][str(channel_number)]

                #             # Calculate kWh & Wh
                #             if channel_usage.usage is not None:
                #                 _wh = ua.DataValue(
                #                     Value=float(channel_usage.usage * 1000),
                #                     StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                #                 )
                #                 _kwh = ua.DataValue(
                #                     Value=float(channel_usage.usage),
                #                     StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                #                 )
                #                 _percentage = ua.DataValue(
                #                     Value=float(channel_usage.percentage),
                #                     StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
                #                 )
                #             else:
                #                 print("NULLLLLLL -----------------")
                #                 _wh = ua.DataValue(
                #                     Value=0.0,
                #                     StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                #                 )
                #                 _kwh = ua.DataValue(
                #                     Value=0.0,
                #                     StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                #                 )
                #                 _percentage = ua.DataValue(
                #                     Value=0.0,
                #                     StatusCode_=ua.StatusCode(ua.StatusCodes.Bad),
                #                 )

                #             await channel_node["usage_kwh"].set_value(_kwh)
                #             await channel_node["usage_wh"].set_value(_wh)
                #             await channel_node["percentage"].set_value(_percentage)
                #             await channel_node["channel_num"].set_value(
                #                 channel_usage.channel_num
                #             )
                #             await channel_node["name"].set_value(channel_usage.name)

                #     else:

                #         await device_node[key].set_value(param)

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

        print("Updating device usage...")

        if ADD_EMPORIA_DEVICES:
            await client.update_emporia_device_usage()  # Fetch the usage data periodically

        if ADD_HUBITAT_DEVICES:
            await client.update_hubitat_device_usage()  # Fetch the usage data periodically

        print("Done updating device usage...")

        await asyncio.sleep(10)  # Adjust sleep time based on your requirements


async def main():
    async with AsyncServer(
        "opc.tcp://0.0.0.0:4840",
        "TriNova AC Project",
    ) as client:

        print("Starting...")

        # Start the update_device_usage function as a background task
        asyncio.create_task(periodic_update(client))

        while True:

            await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
