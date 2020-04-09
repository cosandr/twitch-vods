import asyncio
import json
import logging
import re
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from typing import AsyncIterable, Union

try:
    import cv2
except ImportError:
    print('OpenCV not available')


re_watch = {
    'frame': re.compile(r'frame=(\d+)'),
    'fps': re.compile(r'fps=(\d+\.\d+)'),
    'total_size': re.compile(r'total_size=(\d+)'),
    'out_time': re.compile(r'out_time=(\d{1,2}:\d{2}:\d{2}\.\d+)')
}
re_duration = re.compile(r'(?P<h>\d{1,2}):(?P<m>\d{2}):(?P<s>\d{2})\.(?P<ms>\d+)')


def parse_duration(time_str: str):
    """Parse HH:MM:SS.MICROSECONDS to timedelta"""
    if m := re_duration.match(time_str):
        return timedelta(hours=int(m.group('h')), minutes=int(m.group('m')), seconds=int(m.group('s')))
    return None


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


def read_video_info_cv2(vid_fp: str):
    """Returns video duration (as timedelta) using OpenCV"""
    cap = cv2.VideoCapture(vid_fp)
    total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    total_seconds = int(total_frames / cap.get(cv2.CAP_PROP_FPS))
    cap.release()
    return timedelta(seconds=total_seconds)
    # cap.set(cv2.CAP_PROP_POS_AVI_RATIO, 1)
    # total_ms = cap.get(cv2.CAP_PROP_POS_MSEC)
    # return timedelta(milliseconds=total_ms)


async def run_ffmpeg(logger: logging.Logger, args: list, print_every: int = 30):
    logger.debug('CMD: ffmpeg %s', ' '.join(args))
    p = await asyncio.create_subprocess_exec('ffmpeg', *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        await asyncio.gather(watch_ffmpeg(logger, p.stdout, print_every), watch_ffmpeg(logger, p.stderr, print_every))
    except Exception as e:
        logger.critical('stdout/err critical failure: %s', str(e))
    await p.wait()
    if p.returncode != 0:
        raise Exception(f'ffmpeg exit code: {p.returncode}')


async def watch_ffmpeg(logger: logging.Logger, stream: Union[AsyncIterable, asyncio.StreamReader], print_every: int = 30):
    last_time = 0
    parsed_dict = {}
    try:
        async for line in stream:
            # Add found value to parsed_dict
            for name, regxp in re_watch.items():
                if m := regxp.match(line.decode()):
                    parsed_dict[name] = m.group(1)
                    break
            if len(parsed_dict) == len(re_watch):
                # Log every print_every seconds
                curr_time = parse_duration(parsed_dict['out_time'])
                if curr_time is not None:
                    curr_time = curr_time.total_seconds()
                if curr_time is None or abs(curr_time - last_time) >= print_every:
                    logger.debug('[ffmpeg] frame %.0f, FPS %.2f, time %s, size %.1fMB',
                                 int(parsed_dict["frame"]), float(parsed_dict["fps"]), str(parsed_dict["out_time"]),
                                 int(parsed_dict["total_size"])/1e6)
                    parsed_dict.clear()
                    last_time = curr_time
    except ValueError as e:
        logger.warning('[ffmpeg] Stream Error: %s', str(e))
        pass


def setup_logger(logger: logging.Logger, file_name: str):
    """Adds console handler and rotating file handler at log/<file_name>.log"""
    log_fmt = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(log_fmt)
    # File Handler
    fh = RotatingFileHandler(
        filename=f'log/{file_name}.log',
        maxBytes=int(1e6), backupCount=3,
        encoding='utf-8', mode='a'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(log_fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
