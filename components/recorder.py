#!/usr/bin/python3

import asyncio
import logging
import os
import pickle
import re
import signal
import sys
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler

from aiohttp import ClientSession

import config as cfg


class Recorder:
    time_fmt = '%y%m%d-%H%M'

    def __init__(self, loop: asyncio.BaseEventLoop, logger, client_id: str, user: str, timeout: int, raw_path: str):
        self.loop = loop
        self.logger = logger
        self.client_id = client_id
        self.user = user
        self.timeout = timeout
        self.raw_path = raw_path
        self.check_en = asyncio.Event()
        self.check_en.set()
        self.title = ''
        self.start_time = ''
        self.aio_sess = None
        self.ended_ok = False
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signalnum, frame):
        raise KeyboardInterrupt()

    async def init_sess(self):
        self.logger.info("aiohttp session initialized.")
        self.aio_sess = ClientSession()

    async def close_sess(self):
        await self.aio_sess.close()
        self.logger.info("aiohttp session closed.")

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
        self.start_dt = datetime.now()
        self.logger.info('%s is live: %s', self.user, self.title)
        await self.record()

    async def record(self):
        self.check_en.clear()
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.title)
        start_time_str = self.start_dt.strftime(self.time_fmt)
        rec_name = F"{start_time_str}_{self.user}_{no_space_title}"
        raw_fp = os.path.join(self.raw_path, f'{rec_name}.flv')
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
        start_time = self.start_dt.strftime(self.time_fmt)
        # Title without illegal NTFS characters, no extra spaces and no trailing whitespace
        win_title = re.sub(r'(\s{2,}|\s+$|[<>:\"/\\|?*\n]+)', '', self.title)
        conv_name = F"{start_time}_{win_title}"
        # Send job to transcoder
        send_dict = {'src': raw_fp, 'file_name': conv_name, 'user': self.user}
        try:
            await self.send_job(send_dict)
        except Exception as e:
            self.logger.error('%s: src %s, file_name %s, user %s', str(e), *send_dict.values())
        self.loop.create_task(self.timer())

    async def send_job(self, job: dict):
        _, writer = await asyncio.open_unix_connection(cfg.SOCK, loop=self.loop)
        writer.write(pickle.dumps(job))
        await writer.drain()

    async def run(self, cmd: str):
        p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            await asyncio.gather(self.watch(p.stdout, prefix='STDOUT'), self.watch(p.stderr, prefix='STDERR'))
        except Exception as e:
            self.logger.critical('stdout/err critical failure: %s', str(e))
        await p.wait()
        if p.returncode != 0 and not self.ended_ok:
            raise Exception(f"streamlink exit code: {p.returncode}")
        self.ended_ok = False

    async def watch(self, stream, prefix=''):
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


if __name__ == '__main__':
    # --- Logger ---
    log_fmt = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    logger = logging.getLogger('recorder')
    logger.setLevel(logging.DEBUG)
    if not os.path.exists('log'):
        os.mkdir('log')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(log_fmt)
    fh = RotatingFileHandler(
        filename=f'log/recorder.log',
        maxBytes=int(1e6), backupCount=3,
        encoding='utf-8', mode='a'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(log_fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Twitch recorder started with PID %d", os.getpid())
    user = os.getenv('STREAM_USER')
    if not user:
        logger.critical('A user to check is required')
        sys.exit(0)
    timeout = os.getenv('CHECK_TIMEOUT', '120')
    try:
        timeout = int(timeout)
    except ValueError:
        logger.critical('Invalid timeout %s', timeout)
        sys.exit(0)
    dst_path = '.'
    env_path = os.getenv('PATH_RAW')
    if env_path:
        if not os.path.exists(env_path):
            logger.warning('%s does not exist', env_path)
        else:
            dst_path = env_path
    logger.info('Saving raw files to %s', dst_path)
    loop = asyncio.get_event_loop()
    rec = Recorder(loop, logger, cfg.TWITCH_ID, user=user, timeout=timeout, raw_path=dst_path)
    loop.run_until_complete(rec.init_sess())
    while True:
        try:
            loop.run_until_complete(rec.check_if_live())
        except KeyboardInterrupt:
            print(F"Keyboard interrupt, exit.")
            break
        except Exception as error:
            traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
            pass
    loop.run_until_complete(rec.close_sess())
