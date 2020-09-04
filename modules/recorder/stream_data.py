from datetime import datetime, timezone
from typing import Optional

from dateutil.parser import isoparse

from . import LOGGER


class StreamData:
    """Class representing stream data from Kraken API"""
    def __init__(self, **kwargs):
        self.type: str = kwargs.pop('type_')
        self.created_at: datetime = kwargs.pop('created_at', datetime.now())
        self._created_at_str: str = ''
        self.preview: str = kwargs.pop('preview', '')
        self.user_logo: str = kwargs.pop('user_logo', '')
        self.title: str = kwargs.pop('title', '')
        self.url: str = kwargs.pop('url', '')

    @property
    def created_at_str(self) -> str:
        return self._created_at_str

    @created_at_str.setter
    def created_at_str(self, time_fmt: str):
        """Format created_at with given time format string"""
        self._created_at_str = self.created_at.strftime(time_fmt)

    @classmethod
    def from_json(cls, data: Optional[dict]):
        if not data or not data.get('stream'):
            return None
        data = data['stream']
        kwargs = {'type_': data['stream_type']}
        if time_str := data.get('created_at'):
            try:
                kwargs['created_at'] = isoparse(time_str)
                # Convert UTC to local time
                kwargs['created_at'] = kwargs['created_at'].replace(tzinfo=timezone.utc).astimezone(tz=None)
            except Exception as e:
                LOGGER.warning(f'Cannot parse time string "{time_str}": {e}')
        kwargs['preview'] = data['preview']['medium']
        if data.get('channel'):
            kwargs['url'] = data['channel']['url']
            kwargs['user_logo'] = data['channel']['logo']
            if data['channel'].get('status'):
                kwargs['title'] = data['channel']['status'].replace("/", "")
        return cls(**kwargs)
