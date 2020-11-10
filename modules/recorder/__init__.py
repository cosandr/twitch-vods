import json


class InvalidResponseError(Exception):
    def __init__(self, status: int, reason: str, data: dict):
        self.status = status
        self.reason = reason
        self.data = data
        self.data_str = json.dumps(self.data, indent=2)

    def __str__(self):
        return f'Invalid response {self.status} {self.reason}'


from .stream_data import StreamData
from .user_data import UserData
from .recorder import Recorder
