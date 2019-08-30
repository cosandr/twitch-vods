import argparse
import asyncio
import logging
import os
import re
import sys
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler

from aiohttp import ClientSession

import config as cfg

def parse_args():
    parser = argparse.ArgumentParser(description='Twitch live stream recorder.')
    parser.add_argument('-u', '--user', required=True, type=str,
                        help='Twitch username to check.')
    parser.add_argument('-t', '--timeout', required=False, type=int,
                        help='How often to check, keep longer than 60s.',
                        default=120)

    args = vars(parser.parse_args())
    return args


class Check():
    title = ''
    start_time = ''
    aio_sess = None
    timer_task = None
    flv_vid = ''
    mp4_vid = ''

    def __init__(self, loop: asyncio.BaseEventLoop, client_id: str, user: str, timeout: int=120):
        self.client_id = client_id
        self.user = user
        self.timeout = timeout
        self.loop = loop
        self.check_en = asyncio.Event(loop=loop)
        self.check_en.set()
        ### Logger ###
        self.logger = logging.getLogger('twitch')
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists('log'):
            os.mkdir('log')
        fh = RotatingFileHandler(
            filename='log/twitch_async.log',
            maxBytes=1e6, backupCount=3,
            encoding='utf-8', mode='a'
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.logger.info("Twitch recorder started with PID %d", os.getpid())
    
    async def init_sess(self):
        self.logger.info(F"aiohttp session initialized.")
        self.aio_sess = ClientSession()
    
    async def close_sess(self):
        await self.aio_sess.close()
        self.logger.info(F"aiohttp session closed.")
    
    async def timer(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        self.logger.debug(F"Starting timer with {timeout}s timeout.")
        for _ in range(timeout):
            await asyncio.sleep(1)
        self.check_en.set()

    async def check_if_live(self):
        headers = {'Client-ID': self.client_id}
        url = "https://api.twitch.tv/helix/streams"
        params = {'user_login': self.user}
        while True:
            await self.check_en.wait()
            data = None
            try:
                async with self.aio_sess.get(url=url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                    else:
                        self.logger.error(F"Invalid response {resp.status}, retrying in {self.timeout}s.")
                        self.loop.create_task(self.timer())
                        self.check_en.clear()
            except Exception as e:
                self.logger.error(F"aiohttp error {str(e)}, retrying in {self.timeout}s.")
                self.loop.create_task(self.timer())
                self.check_en.clear()
            if data and (len(data['data']) != 0) and (data['data'][0]['type'] == 'live'):
                self.title = data['data'][0]['title'].replace("/", "")
                self.start_dt = datetime.now()
                self.logger.debug(F"{self.user} is live: {self.title}")
                await self.record()
            else:
                self.logger.debug(F"{self.user} is offline, retrying in {self.timeout}s.")
                self.loop.create_task(self.timer())
                self.check_en.clear()
                

    async def record(self):
        self.check_en.clear()
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.title)
        start_time_str = self.start_dt.strftime("%y%m%d-%H%M")
        rec_name = F"{start_time_str}_{self.user}_{no_space_title}"
        self.flv_vid = F"{cfg.RAW}/{rec_name}.flv"
        stream_url = F"twitch.tv/{self.user}"
        self.logger.info(F"Saving raw stream to {self.flv_vid}")
        cmd = F"streamlink {stream_url} --default-stream best -o {self.flv_vid} -l info"
        p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await asyncio.gather(self.watch(p.stdout, 'streamlink', prefix='STDOUT:'), self.watch(p.stderr, 'streamlink',  prefix='STDERR:'))
        await p.wait()
        if p.returncode != 0:
            self.logger.error(F"Streamlink non-zero returncode: {p.returncode}")
            if not os.path.exists(self.flv_vid):
                self.logger.critical(F"No raw file found {self.flv_vid}")
                self.loop.create_task(self.timer())
                return
            self.logger.info(F"Premature exit but raw file exists.")
        await self.copy(self.flv_vid)

    async def copy(self, source: str):
        start_time = self.start_dt.strftime("%y%m%d-%H%M")
        conv_name = F"{start_time}_{self.title}"
        proc_path = F"{cfg.PROC}/{self.user}"
        if not os.path.exists(proc_path):
            os.mkdir(proc_path)
        self.mp4_vid = F"{proc_path}/{conv_name}.mp4"
        cmd = F'ffmpeg -i {source} -err_detect ignore_err -f mp4 -acodec aac -c copy -y -v info -hide_banner "{self.mp4_vid}"'
        p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=0)
        try:
            await asyncio.gather(self.watch(p.stdout, 'ffmpeg', prefix='STDOUT:'), self.watch(p.stderr, 'ffmpeg',  prefix='STDERR:'))
        except Exception as e:
            self.logger.critical(f'stdout/err critical failure: {str(e)}')
        await p.wait()
        if p.returncode != 0:
            self.logger.error(F"FFMPEG non-zero returncode: {p.returncode}")
        await self.delete_raw()

    async def watch(self, stream, proc, prefix=''):
        async for line in stream:
            tmp = line.decode()
            if 'Opening stream' in tmp:
                self.logger.info(F"Stream opened.")
            elif 'Stream ended' in tmp:
                self.logger.info(F"Stream ended.")
            elif prefix == 'STDERR:':
                self.logger.warning(F"[{proc}] {prefix} {tmp}")
            else:
                self.logger.debug(F"[{proc}] {prefix} {tmp}")

    async def delete_raw(self):
        if not os.path.exists(self.mp4_vid):
            self.logger.critical(F"Raw: {self.flv_vid}\nProcessed [MISSING]: {self.mp4_vid}")
        else:
            flv_size = os.path.getsize(self.flv_vid)
            mp4_size = os.path.getsize(self.mp4_vid)
            flv_size_str = f'{flv_size/1e6:,.1f}MB'
            mp4_size_str = f'{mp4_size/1e6:,.1f}MB'
            # Make sure MP4 size is at least 90% FLV's
            if mp4_size / flv_size < 0.9:
                self.logger.error(f'Processed file was {mp4_size_str} while raw file is {flv_size_str}.')
            else:
                try:
                    os.unlink(self.flv_vid)
                    self.logger.info(f'Raw: {self.flv_vid} [{flv_size_str}]\nDeleted.')
                except Exception as e:
                    self.logger.warning(f'Raw: {self.flv_vid} [{flv_size_str}]\nFailed to delete: {str(e)}.')
        self.check_en.set()

if __name__ == '__main__':
    args = parse_args()
    loop = asyncio.get_event_loop()
    c = Check(loop, cfg.TWITCH_ID, args['user'], timeout=args['timeout'])
    loop.run_until_complete(c.init_sess())
    while True:
        try:
            loop.run_until_complete(c.check_if_live())
        except KeyboardInterrupt:
            print(F"Keyboard interrupt, exit.")
            break
        except Exception as error:
            traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
            pass
    loop.run_until_complete(c.close_sess())
