import asyncio
import logging
import unittest
from typing import Iterable

from utils import watch_ffmpeg


async def to_async_stream(data: Iterable):
    for d in data:
        yield d.encode()


class TestFFMPEG(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logging.getLogger('TestFFMPEG')
        cls.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        cls.logger.addHandler(ch)

    def test_watch(self):
        asyncio.run(self.async_test_watch())

    async def async_test_watch(self):
        ffmpeg_output = """frame=61
fps=36.08
stream_0_0_q=-0.0
bitrate=   8.6kbits/s
total_size=3050
out_time_us=2838000
out_time_ms=2838000
out_time=00:00:02.838000
dup_frames=0
drop_frames=0
speed=1.68x
progress=continue
frame=120
fps=36.08
stream_0_0_q=-0.0
bitrate=   8.6kbits/s
total_size=305000
out_time_us=2838000
out_time_ms=2838000
out_time=00:00:06.838000
dup_frames=0
drop_frames=0
speed=1.68x
progress=continue
frame=1200
fps=42.00
stream_0_0_q=-0.0
bitrate=   8.6kbits/s
total_size=30500000
out_time_us=2838000
out_time_ms=2838000
out_time=00:01:00.000000
dup_frames=0
drop_frames=0
speed=1.69x
progress=continue"""
        await watch_ffmpeg(self.logger, to_async_stream(ffmpeg_output.split('\n')), print_every=0)


if __name__ == '__main__':
    unittest.main()
