#!/usr/bin/python3

import asyncio
import logging
import os
import platform
import re
import sys
import traceback
from datetime import datetime, timedelta
from typing import List

from modules import IntroTrimmer
from utils import read_video_info_cv2

TIME_FMT = '%y%m%d-%H%M'
FFMPEG_COPY = '-i {0} -err_detect ignore_err -f mp4 -c:a aac -c:v copy -y -progress - -nostats -hide_banner {1}'
FFMPEG_HEVC = 'ffmpeg -i "{0}" -c:v libx265 -x265-params crf=23:pools=4 -preset:v fast -c:a aac -v warning -y -progress - -nostats -hide_banner "{1}"'
FFMPEG_HEVC_LIST = '-i {0} -c:v libx265 -x265-params crf=23:pools=6 -preset:v medium -c:a aac -v warning -y -progress - -nostats -hide_banner {1}'
STREAMLINK_LIST = '-l info --default-stream best -o {1} {0}'


async def run_cmd(cmd: str, logger: logging.Logger):
    exec_name = cmd.split(' ')[0]
    p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        await asyncio.gather(watch(p.stdout, exec_name, logger, prefix='STDOUT:'), watch(p.stderr, exec_name, logger, prefix='STDERR:'))
    except Exception as e:
        logger.critical(f'stdout/err critical failure: {str(e)}')
    await p.wait()
    if p.returncode != 0:
        raise Exception(F"{exec_name} exit code: {p.returncode}")


async def run_exec(exec_name, args: list, logger: logging.Logger):
    p = await asyncio.create_subprocess_exec(exec_name, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        await asyncio.gather(watch(p.stdout, exec_name, logger, prefix='STDOUT:'), watch(p.stderr, exec_name, logger, prefix='STDERR:'))
    except Exception as e:
        logger.critical(f'stdout/err critical failure: {str(e)}')
    await p.wait()
    if p.returncode != 0:
        raise Exception(F"{exec_name} exit code: {p.returncode}")


async def watch(stream: asyncio.StreamReader, proc, logger, prefix=''):
    # Can use parse_duration to print every X seconds
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
            # Print if we found all
            if log_dict.keys() == parsed.keys():
                print(f'[ENCODE] frame {log_dict["frame"]:.0f}, {log_dict["fps"]:.2f} fps, time {log_dict["out_time"]}, size {log_dict["size"]/1e6:.1f}MB')
                log_dict.clear()
    except ValueError as e:
        logger.warning(F"[{proc}] STREAM: {str(e)}")
        pass


def get_stream_url(url: str):
    from streamlink import Streamlink, PluginError, NoPluginError
    # Create the Streamlink session
    streamlink = Streamlink()

    # Enable logging
    streamlink.set_loglevel("info")
    streamlink.set_logoutput(sys.stdout)

    # Attempt to fetch streams
    try:
        streams = streamlink.streams(url)
    except NoPluginError:
        exit("Streamlink is unable to handle the URL '{0}'".format(url))
    except PluginError as err:
        exit("Plugin error: {0}".format(err))

    if not streams:
        exit("No streams found on URL '{0}'".format(url))

    # We found the stream
    return streams['best'].url


def read_env():
    # Read env vars
    # Set to None if it is required
    env_vars = {
        'STREAM_USER': None,
        'CHECK_TIMEOUT': '120',
        'PATH_RAW': '.',
    }
    use_vars = {}
    for name, default in env_vars.items():
        tmp = os.getenv(name)
        if not tmp and default is None:
            logger.critical('%s is required', name)
            # Exit 0 so docker/systemd doesn't try to restart on failure
            sys.exit(0)
        elif tmp:
            use_vars[name] = tmp
        else:
            use_vars[name] = default


async def find_concat(logger: logging.Logger, path: str, use_mtime: bool = True) -> List[str]:
    """Find files which should be concatenated"""
    # ffmpeg -f concat -safe 0 -i concat.txt -c copy output.mp4
    re_time = re.compile(r'\d{6}-\d{4}')
    tol = timedelta(minutes=30).total_seconds()
    ret = []
    tmp = []
    prev_name = ''
    prev_end = datetime.now()
    for f in sorted(os.listdir(path)):
        m = re_time.search(f)
        if not m:
            logger.warning('Could not extract time string from %s', f)
            continue
        file_name, _ = os.path.splitext(f)
        curr_name = file_name.split('_', 1)[1]
        if curr_name != prev_name:
            logger.debug('Name mismatch %s != %s', curr_name, prev_name)
            prev_name = curr_name
            tmp = [curr_name]
            continue
        fp = os.path.join(path, f)
        start_dt = datetime.strptime(m.group(), TIME_FMT)
        if use_mtime:
            curr_end = datetime.fromtimestamp(os.path.getmtime(fp))
        else:
            # curr_dur = await read_video_info(fp, logger)
            # curr_end = start_dt + curr_dur
            curr_dur = read_video_info_cv2(fp)
            curr_end = start_dt + curr_dur
        logger.debug('%s ended at about %s', file_name, str(curr_end))
        time_diff = start_dt - prev_end
        time_diff_sec = abs(time_diff.total_seconds())
        if time_diff_sec <= tol:
            tmp.append(f)
        elif len(tmp) == 1:
            tmp.clear()
        elif len(tmp) > 1:
            logger.info('Found %d to concat', len(tmp))
            concat_str = ''
            for t in tmp:
                concat_str += f"file '{t}'\n"
            logger.debug('%s', concat_str)
            ret.append(concat_str)
            tmp.clear()
        prev_end = curr_end
        prev_name = curr_name
    return ret


if __name__ == "__main__":
    logger = logging.getLogger('test')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    loop = asyncio.get_event_loop()
    url = 'https://www.twitch.tv/esl_csgo'
    # c = Check(loop, cfg.TWITCH_ID, user='esl_csgo', timeout=120)
    # loop.run_until_complete(c.init_sess())
    # cmd = FFMPEG_HEVC.format('/tank/media/test/Doctor.Strange.CLIP.mkv', 'output.mkv')
    # args = FFMPEG_HEVC_LIST.format('/tank/media/test/Doctor.Strange.CLIP.mkv', 'output.mkv').split(' ')
    # args_streamlink = STREAMLINK_LIST.format(url, 'stream.flv').split(' ')
    # args_stream = FFMPEG_COPY.format(get_stream_url(url), 'stream.mp4').split(' ')
    crop = IntroTrimmer(debug=2, cfg_path='data/trimmer/config.json')
    media_path = '/tank/media'
    if platform.system() == 'Windows':
        media_path = 'T:/media'
    elif platform.node() != 'DreSRV':
        media_path = f'/dresrv{media_path}'
    in_file = f'{media_path}/twitch/RichardLewisReports/200220-2246_Return Of By The Numbers #108.mp4'
    out_file = f'{media_path}/twitch_raw/test.mp4'
    try:
        # loop.run_until_complete(crop.run_crop(in_file, out_file))
        # crop.find_intro('/tank/media/twitch/RichardLewisReports/200219-2045_Soviet Bloviate.mkv')
        loop.run_until_complete(find_concat(logger, f'{media_path}/twitch/RichardLewisReports', use_mtime=False))
    except KeyboardInterrupt:
        print(F"Keyboard interrupt, exit.")
    except Exception as error:
        traceback.print_exception(type(error), error, error.__traceback__)
        pass
    # loop.run_until_complete(c.close_sess())
