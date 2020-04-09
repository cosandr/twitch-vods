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

    def __init__(self, loop: asyncio.AbstractEventLoop, convert_non_btn=False):
        self.loop = loop
        # --- Logger ---
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'encoder')
        # --- Jobs ---
        self.jobs: List[dict] = []
        self._ready = asyncio.Event()
        self.dst_path = '.'
        self._was_trimmed = False
        # --- Jobs ---
        self.re_btn = re.compile(r'by\s*the\s*numbers', re.IGNORECASE)
        self.server: asyncio.AbstractServer = None
        self.cropper = Cropper()
        self.convert_non_btn = convert_non_btn
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.get_env()
        self.loop.run_until_complete(self.async_init())
        self.read_jobs()
        self.logger.info("Encoder started with PID %d", os.getpid())

    def signal_handler(self, signalnum, frame):
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

    #region Job read/write
    def read_jobs(self):
        if not os.path.exists(self.jobs_file):
            return
        with open(self.jobs_file, 'r', encoding='utf-8') as fr:
            self.jobs = json.load(fr)
        self.logger.info('Jobs file read')

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
        self.write_jobs()
        self.server.close()
        await self.server.wait_closed()
        self.logger.info("Socket closed")
        if os.path.exists(cfg.SOCKET_FILE):
            os.unlink(cfg.SOCKET_FILE)
            self.logger.info(f"Socket file removed: {cfg.SOCKET_FILE}")

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
        msg['raw'] = True
        msg['ignore'] = False
        self.jobs.append(msg)
        self.write_jobs()
        self._ready.set()
    #endregion

    async def job_wait(self):
        await self._ready.wait()
        # Clear if this is the last job
        more_jobs = False
        for j in self.jobs:
            if j['raw'] and not j.get('ignore'):
                more_jobs = True
                break
        if not more_jobs:
            self._ready.clear()
        j = self.jobs[-1]
        self._was_trimmed = False
        # Create folder from username if needed
        out_path = os.path.join(self.dst_path, j['user'])
        if not os.path.exists(out_path):
            os.mkdir(out_path)
        # Encode to HEVC if not BTN episode
        is_btn = self.re_btn.search(j['file_name'])
        if is_btn:
            cmd = self.copy_args.copy()
            j['enc_file'] = j['file_name'] + '.mp4'
            j['enc_codec'] = 'copy'
        elif self.convert_non_btn:
            cmd = self.hevc_args.copy()
            j['enc_file'] = j['file_name'] + '.mkv'
            j['enc_codec'] = 'hevc'
        else:
            # Don't do anything to non-BTN files
            j['ignore'] = True
            return
        out_fp = os.path.join(out_path, j['enc_file'])
        # Insert input
        cmd.insert(0, '-i')
        cmd.insert(1, j['src'])
        # Append output file
        cmd.append(out_fp)
        self.logger.info('Encoding %s -> %s', j['src'], out_fp)
        # Crop if BTN
        if is_btn:
            try:
                intro_seconds = self.cropper.find_intro(j['src'])
                cmd.insert(0, '-ss')
                cmd.insert(1, intro_seconds)
                self.logger.info('Trimming, starting at %d seconds', intro_seconds)
                self._was_trimmed = True
                j['trimmed'] = intro_seconds
            except Exception as e:
                self.logger.error('Could not find intro seconds: %s', str(e))
        # Try to encode
        try:
            j['enc_cmd'] = cmd
            j['enc_start'] = datetime.utcnow().isoformat()
            await run_ffmpeg(logger=self.logger, args=cmd)
            self.logger.info('Encoded: %s', out_fp)
            j['enc_end'] = datetime.utcnow().isoformat()
            j['raw'] = False
        except Exception as e:
            self.logger.error('Encoding failed: %s', str(e))
            j['failure'] = str(e)
            j['ignore'] = True
        # Try to delete raw
        if not j.get('failure'):
            try:
                await self.delete_raw(j['src'], out_fp)
                j['deleted'] = True
            except Exception as e:
                j['deleted'] = False
                j['failure'] = str(e)

    async def delete_raw(self, raw_fp: str, proc_fp: str):
        if self._was_trimmed:
            self.logger.info('%s was trimmed, will not delete')
            raise Exception('Video was trimmed')
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
