#!/usr/bin/python3

import asyncio
import json
import logging
import os
import pickle
import re
import signal
import sys
from datetime import datetime

from aiohttp import ClientSession

import config as cfg
from utils import setup_logger


class Recorder:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        # --- Logger ---
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'recorder')
        # --- Logger ---
        self.client_id: str = ''
        self.user: str = ''
        self.timeout: int = 120
        self.dst_path: str = '.'
        self.check_en = asyncio.Event()
        self.check_en.set()
        self.title: str = ''
        self.start_time: datetime = None
        self.start_time_str: str = ''
        self.writer: asyncio.StreamWriter = None
        # IP of encoder when using TCP
        self.tcp_host: str = '127.0.0.1'
        self.aio_sess = None
        self.ended_ok = False
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.get_env()
        self.loop.run_until_complete(self.async_init())
        self.logger.info("Twitch recorder started with PID %d", os.getpid())

    def signal_handler(self, signalnum, frame):
        raise KeyboardInterrupt()

    def get_env(self):
        """Updates configuration from env variables"""
        self.user = os.getenv('STREAM_USER')
        self.client_id = os.getenv('TWITCH_ID')
        if not self.user:
            self.logger.critical('A user to check is required')
            sys.exit(0)
        if not self.client_id:
            self.logger.critical('Twitch client ID is required')
            sys.exit(0)
        if timeout := os.getenv('CHECK_TIMEOUT'):
            try:
                self.timeout = int(timeout)
            except ValueError:
                self.logger.error('Invalid timeout %s', timeout)
        self.logger.info('Checking %s every %d seconds', self.user, self.timeout)
        if env_path := os.getenv('PATH_RAW'):
            if not os.path.exists(env_path):
                self.logger.warning('%s does not exist', env_path)
            else:
                self.dst_path = env_path
        self.logger.info('Saving raw files to %s', self.dst_path)
        if tcp_host := os.getenv('TCP_HOST'):
            self.tcp_host = tcp_host

    async def async_init(self):
        self.logger.info("aiohttp session initialized.")
        self.aio_sess = ClientSession()
        for i in range(3):
            try:
                if cfg.TCP:
                    self.logger.info("Connecting to TCP server at %s:%d", self.tcp_host, cfg.TCP_PORT)
                    _, self.writer = await asyncio.open_connection(self.tcp_host, cfg.TCP_PORT)
                else:
                    self.logger.info(f'Connecting to Unix socket at {cfg.SOCKET_FILE}')
                    _, self.writer = await asyncio.open_unix_connection(cfg.SOCKET_FILE)
                self.logger.info("Connected to encoder")
                break
            except Exception as e:
                if i == 2:
                    self.logger.error("Could not connect to encoder, files will not be processed: %s", str(e))
                else:
                    self.logger.error("Could not connect to encoder, retrying: %s", str(e))
                    await asyncio.sleep(1)

    async def close(self):
        await self.aio_sess.close()
        self.logger.info("aiohttp session closed.")
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def timer(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        await asyncio.sleep(timeout)
        self.check_en.set()

    async def aio_request(self, url, headers, params):
        data = None
        try:
            async with self.aio_sess.get(url=url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                else:
                    self.logger.error('Invalid response %d, retrying in %ds', resp.status, self.timeout)
                    self.loop.create_task(self.timer())
                    self.check_en.clear()
        except Exception as e:
            self.logger.error('aiohttp error %s, retrying in %ds', str(e), self.timeout)
            self.loop.create_task(self.timer())
            self.check_en.clear()
        return data

    async def check_if_live(self):
        headers = {'Client-ID': self.client_id}
        url = "https://api.twitch.tv/helix/streams"
        params = {'user_login': self.user}
        await self.check_en.wait()
        data = await self.aio_request(url, headers, params)
        if not data or (len(data['data']) == 0) or (data['data'][0]['type'] != 'live'):
            self.logger.debug('%s is offline, retrying in %ds', self.user, self.timeout)
            self.loop.create_task(self.timer())
            self.check_en.clear()
            return
        # Check if hosting
        url = "https://tmi.twitch.tv/hosts"
        params = {'include_logins': 1, 'host': data['data'][0]['user_id']}
        data_host = await self.aio_request(url=url, headers=None, params=params)
        host_login = data_host['hosts'][0].get('target_login')
        if host_login:
            self.logger.info('%s is live but hosting %s, retrying in 10m.', self.user, host_login)
            self.loop.create_task(self.timer(600))
            self.check_en.clear()
            return
        self.title = data['data'][0]['title'].replace("/", "")
        self.start_time = datetime.now()
        self.start_time_str = self.start_time.strftime(cfg.TIME_FMT)
        self.logger.info('%s is live: %s', self.user, self.title)
        await self.record()

    async def record(self):
        self.check_en.clear()
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.title)
        rec_name = F"{self.start_time_str}_{self.user}_{no_space_title}"
        raw_fp = os.path.join(self.dst_path, f'{rec_name}.flv')
        stream_url = F"twitch.tv/{self.user}"
        self.logger.info('Saving raw stream to %s', raw_fp)
        cmd = F"streamlink {stream_url} --default-stream best -o {raw_fp} -l info"
        try:
            await self.run(cmd)
        except Exception as e:
            self.logger.error('Recording failed %s', str(e))
            if not os.path.exists(raw_fp):
                self.logger.critical('No raw file found %s', raw_fp)
                self.loop.create_task(self.timer())
                return
            self.logger.info("Premature exit but raw file exists.")
        await self.post_record(raw_fp)

    async def post_record(self, raw_fp: str):
        if self.writer:
            # Title without illegal NTFS characters, no extra spaces and no trailing whitespace
            win_title = re.sub(r'(\s{2,}|\s+$|[<>:\"/\\|?*\n]+)', '', self.title)
            conv_name = F"{self.start_time_str}_{win_title}"
            # Send job to encoder
            send_dict = {'src': raw_fp, 'file_name': conv_name, 'user': self.user, 'raw': True}
            try:
                await self.send_job(send_dict)
            except Exception as e:
                self.logger.error('%s: src %s, file_name %s, user %s', str(e), *send_dict.values())
        self.loop.create_task(self.timer())

    async def send_job(self, job: dict):
        if cfg.JSON_SERIALIZE:
            self.writer.write(json.dumps(job).encode('utf-8'))
        else:
            self.writer.write(pickle.dumps(job))
        await self.writer.drain()

    async def run(self, cmd: str):
        p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            await asyncio.gather(self.watch(p.stdout, prefix='STDOUT'), self.watch(p.stderr, prefix='STDERR'))
        except Exception as e:
            self.logger.critical('stdout/err critical failure: %s', str(e))
        await p.wait()
        if p.returncode != 0 and not self.ended_ok:
            raise Exception(f"[streamlink] Non-zero exit code {p.returncode}")
        self.ended_ok = False

    async def watch(self, stream: asyncio.StreamReader, prefix=''):
        try:
            async for line in stream:
                tmp = line.decode()
                if 'Opening stream' in tmp:
                    self.logger.info("Stream opened.")
                elif 'Stream ended' in tmp:
                    self.ended_ok = True
                    self.logger.info("Stream ended.")
                elif prefix == 'STDERR':
                    self.logger.warning('[streamlink] %s: %s', prefix, tmp)
                else:
                    self.logger.debug('[streamlink] %s: %s', prefix, tmp)
        except ValueError as e:
            self.logger.warning('[streamlink] STREAM: %s', str(e))
            pass
