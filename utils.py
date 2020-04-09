import asyncio
import json
import logging
import re
from datetime import timedelta

try:
    import cv2
except ImportError:
    print('OpenCV not available')


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
        logger.critical(f'stdout/err critical failure: {str(e)}')
    await p.wait()
    if p.returncode != 0:
        raise Exception(F"ffmpeg exit code: {p.returncode}")


async def watch_ffmpeg(logger: logging.Logger, stream: asyncio.StreamReader, print_every: int = 30):
    last_time = 0
    log_dict = {}
    try:
        async for line in stream:
            tmp = line.decode()
            # Parse output
            parsed = {
                'frame': tmp.split('frame='),
                'fps': tmp.split('fps='),
                'size': tmp.split('total_size='),
                'out_time': tmp.split('out_time=')
            }
            # Add found value to log_dict
            for k, v in parsed.items():
                if len(v) > 1:
                    try:
                        log_dict[k] = float(v[1])
                    except ValueError:
                        log_dict[k] = v[1].replace('\n', '')
                    break
            if log_dict.keys() == parsed.keys():
                # Log every print_every seconds
                curr_time = parse_duration(log_dict['out_time'])
                if curr_time is not None:
                    curr_time = curr_time.total_seconds()
                if curr_time is None or abs(curr_time - last_time) >= print_every:
                    logger.debug('[ffmpeg] frame %.0f, FPS %.2f, time %s, size %.1fMB',
                                 log_dict["frame"], log_dict["fps"], str(log_dict["out_time"]),
                                 log_dict["size"]/1e6)
                    log_dict.clear()
                    last_time = curr_time
    except ValueError as e:
        logger.warning('[ffmpeg] Stream Error: %s', str(e))
        pass
