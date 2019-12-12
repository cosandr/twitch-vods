import asyncio
import json
import re
from datetime import timedelta


def parse_duration(time_str: str):
    """Parse HH:MM:SS.MICROSECONDS to timedelta"""
    m = re.match(r'(?P<h>\d{1,2}):(?P<m>\d{2}):(?P<s>\d{2})\.(?P<ms>\d+)', time_str)
    if m is None:
        return
    td = timedelta(hours=int(m.group('h')), minutes=int(m.group('m')), seconds=int(m.group('s')))
    return td


async def read_video_info(vid_fp: str, logger=None):
    """Returns video duration (as timedelta) using ffprobe"""
    args = ['-v', 'quiet', '-print_format', 'json', '-show_streams', '-sexagesimal', vid_fp]
    p = await asyncio.create_subprocess_exec('ffprobe', *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, _ = await p.communicate()
    if p.returncode != 0:
        err = f'Cannot get video info for {vid_fp}'
        if logger:
            logger.error(err)
        else:
            print(err)
        return
    # Find duration
    metadata = json.loads(stdout.decode())
    for stream in metadata['streams']:
        if stream['codec_type'] != 'video':
            continue
        # Good for H264
        dur = stream.get('duration')
        # H265
        if dur is None and stream.get('tags') is not None:
            dur = stream['tags'].get('DURATION')
        if dur is None:
            return
        return parse_duration(dur)
    return
