import asyncio
import logging
import pprint
import traceback

from asyncua import Server, ua
from asyncua.ua import NodeId
from asyncua.ua.uaerrors._auto import BadNodeIdExists

from helpers import bcolors
from hubitat import HubitatClient

_logger = logging.getLogger(__name__)

HUBITAT_PORT = 4842


class AsyncServer:

    uri: str
    index: int
    hubitat: HubitatClient
    device_nodes: dict = {}

    def __init__(self, endpoint, name):
        self.server = Server()
        self.server.set_server_name(name)
        self.server.set_endpoint(endpoint)
        self.hubitat = HubitatClient()

    async def init(self):

        await self.server.init()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        await self.server.set_application_uri("urn:trinova.com:Trinova:opcua")

        self.uri = "http://examples.freeopcua.github.io"
        self.index = await self.server.register_namespace(self.uri)

        await self.build_hubitat_objects()

        print(bcolors.OKBLUE + "DONE " + bcolors.ENDC + "Node IDs created...")

    async def build_hubitat_objects(self):

        # Create a parent folder to hold devices
        hubitat_devices_folder = await self.server.nodes.objects.add_folder(
            self.index,
            "Hubitat Devices",
        )

        self.hubitat.get_devices()

        for device in self.hubitat.devices:

            # print(device)

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

                # pprint.pprint(_device_attributes)

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

                    elif _attribute_type == "STRING" or _attribute_type == "ENUM":
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

                        print(
                            bcolors.OKGREEN
                            + "SUCCESS "
                            + bcolors.ENDC
                            + f"Node ID created: { device_q_name }.{ _attribute_name }"
                        )

                    except BadNodeIdExists:

                        print(
                            bcolors.WARNING
                            + "WARNING "
                            + bcolors.ENDC
                            + f"Node ID already exists, most likely a duplicate attribute: { device_q_name }.{ _attribute_name }"
                        )

                    except Exception as e:

                        print(
                            bcolors.FAIL
                            + "FAILED "
                            + bcolors.ENDC
                            + f"Failed to create Node ID: { device_q_name }.{ _attribute_name }"
                        )

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
                    print(
                        bcolors.WARNING
                        + "WARNING "
                        + bcolors.ENDC
                        + f"No device attributes found in device object: { device_info }"
                    )
                    continue

                _live_device_attributes = device_info["attributes"]

                for attribute in _live_device_attributes:

                    if "name" in attribute:
                        _attribute_name = attribute["name"]
                    else:
                        print(
                            bcolors.WARNING
                            + "WARNING "
                            + bcolors.ENDC
                            + f"No name found in device object: { _live_device_attributes }"
                        )
                        continue

                    if "currentValue" in attribute:
                        _attribute_value = attribute["currentValue"]
                    else:
                        print(
                            bcolors.WARNING
                            + "WARNING "
                            + bcolors.ENDC
                            + f"No currentValue found in device object: { _live_device_attributes }"
                        )
                        continue

                    if "dataType" in attribute:
                        _attribute_type = attribute["dataType"]
                    else:
                        print(
                            bcolors.WARNING
                            + "WARNING "
                            + bcolors.ENDC
                            + f"No dataType found in device object: { _live_device_attributes }"
                        )
                        continue

                    if _attribute_value is None:
                        print(
                            bcolors.WARNING
                            + "WARNING "
                            + bcolors.ENDC
                            + f"Attribute value is null for { _attribute_name } in { device_info['name'] }"
                        )
                        continue

                    if _attribute_type == "NUMBER":
                        _attribute_value = float(_attribute_value)
                        _attribute_varianttype = ua.VariantType.Double

                    elif _attribute_type == "STRING" or _attribute_type == "ENUM":
                        _attribute_value = str(_attribute_value)
                        _attribute_varianttype = ua.VariantType.String
                    else:
                        print(
                            bcolors.WARNING
                            + "WARNING "
                            + bcolors.ENDC
                            + f"Attribute type is not supported for { _attribute_name } in { device_info['name'] }: { _attribute_type }"
                        )
                        continue

                    if _attribute_name in device_node["attributes"]:
                        try:

                            await device_node["attributes"][_attribute_name].set_value(
                                _attribute_value
                            )

                            print(
                                bcolors.OKGREEN
                                + "SUCCESS "
                                + bcolors.ENDC
                                + f"Set { device_node['attributes'][_attribute_name] } = { _attribute_value }"
                            )

                        except Exception as e:

                            print(
                                bcolors.FAIL
                                + "FAILED "
                                + bcolors.ENDC
                                + f"Set { device_node['attributes'][_attribute_name] } = { _attribute_value }"
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

        await client.update_hubitat_device_usage()  # Fetch the usage data periodically

        print(bcolors.OKBLUE + "DONE " + bcolors.ENDC + "Node values updated...")

        await asyncio.sleep(10)  # Adjust sleep time based on your requirements


async def main():
    async with AsyncServer(
        f"opc.tcp://0.0.0.0:{ HUBITAT_PORT }",
        "TriNova AC Project - Hubitat",
    ) as client:

        print(bcolors.OKBLUE + "DONE " + bcolors.ENDC + "Service starting...")

        # Start the update_device_usage function as a background task
        asyncio.create_task(periodic_update(client))

        while True:

            await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
