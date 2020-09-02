import json
from datetime import datetime
from typing import Optional


class Job:
    def __init__(self, **kwargs):
        # Path to source file
        self.src: str = kwargs.pop('src', None)
        if not self.src:
            raise
        # Output file name
        self.file_name: str = kwargs.pop('file_name')
        # Streamer username
        self.user: str = kwargs.pop('user')
        # Whether or not this file is unprocessed
        self.raw: bool = kwargs.pop('raw', True)
        # Error text
        self.error: Optional[str] = kwargs.pop('error', None)
        # File is marked as ignored, it is subject to auto cleanup
        self.ignore: Optional[bool] = kwargs.pop('ignore', None)
        # Seconds trimmed off the video start
        self.start_seconds: Optional[int] = kwargs.pop('start_seconds', None)
        # True if raw file was deleted
        self.deleted: Optional[bool] = kwargs.pop('deleted', None)
        # FFMPEG command used for encoding this file
        self.enc_cmd: Optional[str] = kwargs.pop('enc_cmd', None)
        # Complete file name of encoded video, no path
        self.enc_file: Optional[str] = kwargs.pop('enc_file', None)
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

    @classmethod
    def from_dict(cls, data: dict):
        if not data:
            return None
        # Backwards compat
        if 'trimmed' in data:
            data['start_seconds'] = data.pop('trimmed')
        return cls(**data)

    def to_dict(self, no_dt=False) -> dict:
        ret = dict(src=self.src, file_name=self.file_name, user=self.user, raw=self.raw)
        if self.error is not None:
            ret['error'] = self.error
        if self.ignore is not None:
            ret['ignore'] = self.ignore
        if self.start_seconds is not None:
            ret['start_seconds'] = self.start_seconds
        if self.deleted is not None:
            ret['deleted'] = self.deleted
        if self.enc_cmd is not None:
            ret['enc_cmd'] = self.enc_cmd
        if self.enc_file is not None:
            ret['enc_file'] = self.enc_file
        if self.enc_codec is not None:
            ret['enc_codec'] = self.enc_codec
        if self.enc_start is not None:
            if no_dt:
                ret['enc_start'] = self.enc_start.isoformat()
            else:
                ret['enc_start'] = self.enc_start
        if self.enc_end is not None:
            if no_dt:
                ret['enc_end'] = self.enc_end.isoformat()
            else:
                ret['enc_end'] = self.enc_end

        return ret

    def to_json(self, **kwargs) -> str:
        """Return json string serialization, arguments are passed to json.dumps"""
        return json.dumps(self.to_dict(no_dt=True), **kwargs)
