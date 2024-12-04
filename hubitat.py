import requests

# URL = "http://192.168.55.51/apps/api/5/devices?access_token=40a0f81c-5825-402f-8457-c33cfa052775"


class HubitatClient:

    access_token: str = "40a0f81c-5825-402f-8457-c33cfa052775"
    endpoint: str = "http://192.168.55.231/apps/api/5/"
    devices: list = []

    def __init__(self) -> None:
        pass

    def get_devices(self):

        url = self.endpoint + "/devices?access_token=" + self.access_token

        response = requests.get(url)

        # Only parse JSON if response status code is 200
        if response.status_code == 200:
            try:
                self.devices = response.json()
            except requests.exceptions.JSONDecodeError as e:
                print("Error decoding JSON:", e)
                return None
        else:
            print("Failed to fetch devices. HTTP Status:", response.status_code)
            return None

    def get_device_info(self, device_id: int):

        url = (
            self.endpoint
            + "/devices/"
            + device_id
            + "?access_token="
            + self.access_token
        )

        response = requests.get(url)

        # Only parse JSON if response status code is 200
        if response.status_code == 200:
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError as e:
                print("Error decoding JSON:", e)
                return []
        else:
            print("Failed to fetch device info. HTTP Status:", response.status_code)
            return []
