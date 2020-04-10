#!/usr/bin/env python3

import asyncio
import json
import logging
import os
import pickle
import re
import signal
from datetime import datetime
from typing import List

import config as cfg
from utils import read_video_info, run_ffmpeg, setup_logger
from .crop import Cropper
from .notifier import Notifier

"""
### Run in shell
ffprobe -v quiet -print_format json -show_streams -sexagesimal ${IN}
IN= ;OUT= ;ffmpeg -i "${IN}" -c:v libx265 -x265-params crf=23:pools=4 -preset:v fast -c:a aac -y -hide_banner "${OUT}"
IN= ;OUT= ;ffmpeg -i "${IN}" -c:v libx265 -x265-params crf=23:pools=4 -preset:v faster -c:a aac -y -progress - -nostats -hide_banner "${OUT}"

### OUTPUT OF -progress - -nostats
frame=61
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
"""


class Encoder:
    """
    JOB = {
        src: <str>            # Path to source file
        file_name: <str>      # File name in the format TIMESTAMP_NAME
        user: <str>           # Streamer username
        raw: <bool>           # Whether or not this file is unprocessed
        ignore: <bool>        # File is marked as ignored, it is subject to auto cleanup
        trimmed: <int>        # Seconds trimmed off the video start
        deleted: <bool>       # True if raw file was deleted
        enc_cmd: <str>        # FFMPEG command used for encoding this file
        enc_file: <str>       # Complete file name of encoded video, no path
        enc_codec: <str>      # copy/h264/hevc
        enc_start: <time>     # Time encode started
        enc_end: <time>       # Time encode finished
        failure: <str>        # Failure text
    }
    """
    jobs_file = 'data/jobs.json'
    job_keys = ['src', 'file_name', 'user']  # Mandatory keys
    copy_args = [
        '-c:v', 'copy', '-f', 'mp4',
        '-c:a', 'aac',
        '-err_detect', 'ignore_err',
        '-v', 'warning', '-y', '-progress', '-', '-nostats', '-hide_banner'
    ]
    hevc_args = [
        '-c:v', 'libx265', '-x265-params', 'crf=23:pools=4', '-preset:v', 'fast',
        '-c:a', 'aac',
        '-v', 'warning', '-y', '-progress', '-', '-nostats', '-hide_banner'
    ]

    def __init__(self, loop: asyncio.AbstractEventLoop, convert_non_btn=False, always_copy=False, print_every: int = 30):
        self.loop = loop
        self.convert_non_btn = convert_non_btn
        self.always_copy = always_copy
        self.print_every = print_every
        # --- Logger ---
        logger_name = self.__class__.__name__
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'encoder')
        # --- Jobs ---
        self.jobs: List[dict] = []
        self._ready = asyncio.Event()
        self.dst_path = '.'
        # --- Jobs ---
        self.re_btn = re.compile(r'by\s*the\s*numbers', re.IGNORECASE)
        # noinspection PyTypeChecker
        self.server: asyncio.AbstractServer = None
        self.cropper = Cropper(log_parent=logger_name)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.get_env()
        self.loop.run_until_complete(self.async_init())
        self.read_jobs()
        self.logger.info("Encoder started with PID %d", os.getpid())
        # Try to add notifier
        self.notifier = None
        try:
            self.notifier = Notifier(loop=self.loop, log_parent=logger_name)
        except:
            self.logger.exception('Notifier not available')

    async def send_notification(self, content: str):
        if not self.notifier:
            return
        try:
            await self.notifier.send(content, name='Twitch Encoder')
        except:
            self.logger.exception('Cannot send notification')

    def signal_handler(self, signal_num, frame):
        raise KeyboardInterrupt()

    def get_env(self):
        """Updates configuration from env variables"""
        env_path = os.getenv('PATH_PROC')
        if env_path:
            if not os.path.exists(env_path):
                self.logger.warning('%s does not exist', env_path)
            else:
                self.dst_path = env_path
        self.logger.info('Saving encoded files to %s', self.dst_path)

    def mark_job(self, job_num: int):
        if job_num >= len(self.jobs):
            raise IndexError("Job number exceeds number of available jobs")
        j = self.jobs[job_num]
        j['raw'] = True
        j['ignore'] = False
        self._ready.set()
        self.logger.info('File %s as raw and not ignored', j["file_name"])

    #region Job read/write
    def read_jobs(self):
        if not os.path.exists(self.jobs_file):
            return
        with open(self.jobs_file, 'r', encoding='utf-8') as fr:
            self.jobs = json.load(fr)
        self.logger.info('Jobs file read')
        self._ready.set()

    def write_jobs(self):
        with open(self.jobs_file, 'w', encoding='utf-8') as fw:
            json.dump(self.jobs, fw, indent=1)
        self.logger.info('Jobs file updated')

    async def async_init(self):
        if cfg.TCP:
            self.logger.info("Attempting to listen on 0.0.0.0:%d", cfg.TCP_PORT)
            self.server = await asyncio.start_server(self.deserialize, '0.0.0.0', cfg.TCP_PORT)
            self.logger.info(F"Server listening on {self.server.sockets[0].getsockname()}")
        else:
            self.logger.info(f'Starting UNIX socket on {cfg.SOCKET_FILE}')
            self.server = await asyncio.start_unix_server(self.deserialize, cfg.SOCKET_FILE)
            self.logger.info(F"Socket at {self.server.sockets[0].getsockname()} started")

    async def close(self):
        """Close server and remove socket file"""
        await self.send_notification('Encoder is closing')
        self.write_jobs()
        self.server.close()
        await self.server.wait_closed()
        self.logger.info("Socket closed")
        if os.path.exists(cfg.SOCKET_FILE):
            os.unlink(cfg.SOCKET_FILE)
            self.logger.info(f"Socket file removed: {cfg.SOCKET_FILE}")
        if self.notifier:
            await self.notifier.close()

    async def deserialize(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Get and check UNIX socket messages"""
        data = await reader.read(1024)
        if cfg.JSON_SERIALIZE:
            msg = json.loads(data.decode('utf-8'))
        else:
            msg = pickle.loads(data)
        self.logger.debug(f"Message received: {msg}")
        # Check message type
        if not isinstance(msg, dict):
            self.logger.error('Expected dict, got %s', type(msg))
            return
        # Make sure we have all required keys
        for k in self.job_keys:
            if k not in msg.keys():
                self.logger.error('Key %s is missing', k)
                return
        # Check paths
        if not os.path.exists(msg['src']):
            self.logger.error('Source file not found: %s', msg['src'])
            return
        self.jobs.append(msg)
        self.write_jobs()
        if not msg.get('ignore'):
            self._ready.set()
    #endregion

    async def job_wait(self):
        await self._ready.wait()
        j = None
        for i, job in enumerate(self.jobs):
            if job['raw'] and not job.get('ignore'):
                j = self.jobs[i]
                break
        if not j:
            self._ready.clear()
            return
        if not os.path.exists(j['src']):
            status = f"Source file not found: {j['src']}"
            await self.send_notification(status)
            self.logger.error(status)
            j['ignore'] = True
            self.write_jobs()
            return
        keep_raw = False
        # Create folder from username if needed
        out_path = os.path.join(self.dst_path, j['user'])
        if not os.path.exists(out_path):
            os.mkdir(out_path)
        # Encode to HEVC if not BTN episode
        is_btn = self.re_btn.search(j['file_name'])
        if is_btn or self.always_copy:
            cmd = self.copy_args.copy()
            j['enc_file'] = j['file_name'] + '.mp4'
            j['enc_codec'] = 'copy'
        elif self.convert_non_btn:
            cmd = self.hevc_args.copy()
            j['enc_file'] = j['file_name'] + '.mkv'
            j['enc_codec'] = 'hevc'
        else:
            # Don't do anything to non-BTN files
            status = f'Ignoring job for {j["file_name"]}'
            self.logger.info(status)
            await self.send_notification(status)
            j['ignore'] = True
            self.write_jobs()
            return
        out_fp = os.path.join(out_path, j['enc_file'])
        # Insert input
        cmd.insert(0, '-i')
        cmd.insert(1, j['src'])
        # Append output file
        cmd.append(out_fp)
        status = f"Encoding {j['src']} -> {out_fp}"
        self.logger.info(status)
        await self.send_notification(status)
        # Crop if BTN
        if is_btn:
            try:
                intro_seconds = self.cropper.find_intro(j['src'])
                cmd.insert(0, '-ss')
                cmd.insert(1, intro_seconds)
                status = f'Trimming, starting at {intro_seconds} seconds'
                self.logger.info(status)
                await self.send_notification(status)
                keep_raw = True
                j['trimmed'] = intro_seconds
            except Exception as e:
                self.logger.exception('Could not find intro seconds')
                await self.send_notification(f'Could not find intro seconds: {str(e)}')
        # Try to encode
        try:
            j['enc_cmd'] = ' '.join(cmd)
            j['enc_start'] = datetime.utcnow().isoformat()
            await run_ffmpeg(logger=self.logger, args=cmd, print_every=self.print_every)
            j['enc_end'] = datetime.utcnow().isoformat()
            status = 'Encoded %s in %s'.format(
                j['file_name'],
                str(datetime.fromisoformat(j['enc_end'])-datetime.fromisoformat(j['enc_start']))
            )
            self.logger.info(status)
            await self.send_notification(status)
            j['raw'] = False
        except Exception as e:
            await self.send_notification(f'Encoding failed: {str(e)}')
            self.logger.exception('Encoding failed')
            j['failure'] = str(e)
            j['ignore'] = True
        # Try to delete raw
        if not keep_raw and not j.get('failure'):
            try:
                await self.delete_raw(j['src'], out_fp)
                await self.send_notification(f'Raw file deleted: {j["src"]}')
            except Exception as e:
                j['failure'] = str(e)
                await self.send_notification(f'Delete raw failed: {str(e)}')
        j['deleted'] = not os.path.exists(j['src'])
        self.write_jobs()

    async def delete_raw(self, raw_fp: str, proc_fp: str):
        if not os.path.exists(proc_fp):
            self.logger.critical('%s -> MISSING %s', raw_fp, proc_fp)
            raise FileNotFoundError(proc_fp)
        raw_size = os.path.getsize(raw_fp)
        proc_size = os.path.getsize(proc_fp)
        raw_size_str = f'{raw_size/1e6:,.1f}MB'
        proc_size_str = f'{proc_size/1e6:,.1f}MB'
        raw_dur = await read_video_info(raw_fp, self.logger)
        proc_dur = await read_video_info(proc_fp, self.logger)
        if raw_dur is None:
            self.logger.warning('Cannot parse duration: %s', raw_fp)
            raise Exception('Cannot parse raw duration')
        elif proc_dur is None:
            self.logger.warning('Cannot parse duration: %s', proc_fp)
            raise Exception('Cannot parse processed duration')
        dur_diff = raw_dur - proc_dur
        if dur_diff.total_seconds() > 2:
            self.logger.warning('%s [%s] -> SHORTER %s [%s]', raw_dur, raw_size_str, proc_dur, proc_size_str)
            raise Exception(f'{proc_fp} is too short: {proc_dur}')
        try:
            os.unlink(raw_fp)
            self.logger.info('Deleted: %s [%s]', raw_fp, raw_size_str)
        except Exception as e:
            self.logger.error('Failed to delete %s [%s]: %s', raw_fp, raw_size_str, str(e))
            raise
