import json

from aiohttp import web


class Response:
    def __init__(self, data=None, error='', time=0):
        self._web = web.Response(content_type="application/json")
        # An error string
        self.error: str = error
        # How long the request took to complete
        self.time: float = time
        # Return data, probably a dict or string
        self.data = data

    def to_json(self) -> str:
        send_dict = {}
        if self.data:
            send_dict["data"] = self.data
        if self.error:
            send_dict["error"] = self.error
        if self.time:
            send_dict["time"] = self.time
        return json.dumps(send_dict)

    @property
    def status(self) -> int:
        return self._web.status

    @status.setter
    def status(self, val: int):
        self._web.set_status(val)

    @property
    def web_response(self) -> web.Response:
        """Return internal web response"""
        self._web.text = self.to_json()
        return self._web

    def to_web(self, **kwargs) -> web.Response:
        """Return aiohttp web response, keyword args are passed to web.Response constructor"""
        return web.Response(text=self.to_json(), content_type="application/json", **kwargs)
