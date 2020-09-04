import json
import os
from datetime import datetime
from typing import Optional


class Job:
    def __init__(self, **kwargs):
        # Input file name
        self.input: str = kwargs.pop('input')
        # File name
        self.title: str = kwargs.pop('title')
        # Streamer username
        self.user: str = kwargs.pop('user')
        # File created timestamp
        self.created_at: Optional[datetime] = kwargs.pop('created_at', None)
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)
        # Output file name
        self.out_file: Optional[str] = kwargs.pop('out_file', None)
        # True if raw file was deleted
        self.deleted: Optional[bool] = kwargs.pop('deleted', None)
        # File is marked as ignored, it is subject to auto cleanup
        self.ignored: Optional[bool] = kwargs.pop('ignored', None)
        # copy/hevc
        self.enc_codec: Optional[str] = kwargs.pop('enc_codec', None)
        # Time encode started
        self.enc_start: Optional[datetime] = kwargs.pop('enc_start', None)
        if isinstance(self.enc_start, str):
            self.enc_start = datetime.fromisoformat(self.enc_start)
        # Time encode finished
        self.enc_end: Optional[datetime] = kwargs.pop('enc_end', None)
        if isinstance(self.enc_end, str):
            self.enc_end = datetime.fromisoformat(self.enc_end)
        # Seconds trimmed off the video start
        self.start_seconds: Optional[int] = kwargs.pop('start_seconds', None)
        # FFMPEG command used for encoding this file
        self.ffmpeg_args: Optional[str] = kwargs.pop('ffmpeg_args', None)
        # Error text
        self.error: Optional[str] = kwargs.pop('error', None)

    def __repr__(self):
        d = {}
        for k, v in self.__dict__.items():
            if v is not None:
                d[k] = v
        return str(d)

    @classmethod
    def from_dict(cls, data: dict):
        if not data:
            return None
        # Backwards compat
        if 'trimmed' in data:
            data['start_seconds'] = data.pop('trimmed')
        if 'ignore' in data:
            data['ignored'] = data.pop('ignore')
        if 'file_name' in data:
            data['title'] = data.pop('file_name').split('_', 1)[1]
        if 'src' in data:
            data['input'] = os.path.basename(data.pop('src'))
        if 'enc_cmd' in data:
            data['ffmpeg_args'] = data.pop('enc_cmd')
        if 'enc_file' in data:
            data['out_file'] = data.pop('enc_file')
        return cls(**data)

    def to_dict(self, no_dt=False) -> dict:
        d = {}
        for k, v in self.__dict__.items():
            if v is None:
                continue
            if no_dt and isinstance(v, datetime):
                d[k] = v.isoformat()
            else:
                d[k] = v
        return d

    def to_json(self, **kwargs) -> str:
        """Return json string serialization, arguments are passed to json.dumps"""
        return json.dumps(self.to_dict(no_dt=True), **kwargs)
