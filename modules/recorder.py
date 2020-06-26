import asyncio
import inspect
import json
import logging
import os
import pickle
import re
import signal
import sys
from datetime import datetime, timezone

from aiohttp import ClientSession
from dateutil.parser import isoparse
from discord import Embed, Colour

import config as cfg
from utils import setup_logger
from .notifier import Notifier

NAME = 'Twitch Recorder'
ICON_URL = 'https://www.dresrv.com/icons/twitch-recorder.png'


class Recorder:
    dumps_path = 'log/dumps'

    def __init__(self, loop: asyncio.AbstractEventLoop, enable_notifications=True):
        self.loop = loop
        # --- Logger ---
        logger_name = self.__class__.__name__
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'recorder')
        # --- Logger ---
        self.client_id: str = ''
        self.user_login: str = ''
        self.user_id: int = 0
        self.timeout: int = 120
        self.dst_path: str = '.'
        self.check_en = asyncio.Event()
        self.check_en.set()
        self.title: str = ''
        self.start_time: datetime = None
        self.start_time_str: str = ''
        # IP of encoder when using TCP
        self.tcp_host: str = '127.0.0.1'
        self.aio_sess = None
        self.ended_ok = False
        self.notifier = None
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.get_env()
        self.loop.run_until_complete(self.async_init())
        if enable_notifications:
            try:
                self.notifier = Notifier(loop=self.loop, log_parent=logger_name, sess=self.aio_sess)
            except:
                pass
        embed = self.make_embed()
        embed.colour = Colour.light_grey()
        embed.description = 'Started'
        self.loop.create_task(self.send_notification(embed=embed))
        self.logger.info("Twitch recorder started with PID %d", os.getpid())

    async def async_init(self):
        self.logger.info("aiohttp session initialized.")
        self.aio_sess = ClientSession()
        await self.get_user_id()

    async def close(self):
        embed = self.make_embed()
        embed.colour = Colour.orange()
        embed.description = 'Closing'
        await self.send_notification(embed=embed)
        await self.aio_sess.close()
        self.logger.info("aiohttp session closed")

    def signal_handler(self, signal_num, frame):
        self.loop.run_until_complete(self.close())
        exit(0)

    def make_embed(self) -> Embed:
        embed = Embed(title=self.user_login, description=f'Recording {self.title}')
        embed.set_author(name=NAME, icon_url=ICON_URL)
        return embed

    def make_embed_error(self, description: str, e: Exception = None) -> Embed:
        embed = self.make_embed()
        embed.colour = Colour.red()
        embed.description = description
        if e:
            embed.add_field(name='Error', value=str(e), inline=False)
        return embed

    async def send_notification(self, content: str = '', embed: Embed = None):
        if not self.notifier:
            return
        try:
            if not embed and content:
                embed = self.make_embed()
                embed.description = content
            await self.notifier.send(embed=embed)
        except:
            self.logger.exception('Cannot send notification')

    def get_env(self):
        """Updates configuration from env variables"""
        self.user_login = os.getenv('STREAM_USER')
        self.client_id = os.getenv('TWITCH_ID')
        if not self.user_login:
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
        self.logger.info('Checking %s every %d seconds', self.user_login, self.timeout)
        if env_path := os.getenv('PATH_DST'):
            if not os.path.exists(env_path):
                self.logger.warning('%s does not exist', env_path)
            else:
                self.dst_path = env_path
        self.logger.info('Saving raw files to %s', self.dst_path)
        if tcp_host := os.getenv('TCP_HOST'):
            self.tcp_host = tcp_host

    async def open_conn(self) -> asyncio.StreamWriter:
        try:
            if cfg.TCP:
                self.logger.debug("Connecting to TCP server at %s:%d", self.tcp_host, cfg.TCP_PORT)
                _, writer = await asyncio.open_connection(self.tcp_host, cfg.TCP_PORT)
            else:
                self.logger.debug(f'Connecting to Unix socket at {cfg.SOCKET_FILE}')
                _, writer = await asyncio.open_unix_connection(cfg.SOCKET_FILE)
            self.logger.info("Connected to encoder")
            return writer
        except Exception as e:
            self.logger.error("Could not connect to encoder: %s", str(e))
            raise

    async def timer(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        await asyncio.sleep(timeout)
        self.check_en.set()

    def dump_response(self, resp: dict, endpoint: str):
        try:
            if not os.path.exists(self.dumps_path):
                os.mkdir(self.dumps_path)
            file_name = f'{datetime.now().timestamp()}_{self.user_login}_{self.user_id}_{endpoint}.json'
            with open(os.path.join(self.dumps_path, file_name), 'w', encoding='utf-8') as fw:
                json.dump(resp, fw)
            self.logger.debug(f'Dumped response to {file_name}')
        except:
            self.logger.exception(f'Cannot dump response: {resp}')

    async def get_user_id(self):
        """Sets the user's ID"""
        url = 'https://api.twitch.tv/kraken/users'
        headers = {'Client-ID': self.client_id, 'Accept': 'application/vnd.twitchtv.v5+json'}
        params = {'login': self.user_login}
        data = await self.aio_request(url=url, headers=headers, params=params)
        # API call didn't work
        if not data:
            await self.close()
            exit(0)
        for u in data.get('users', []):
            if u['name'].lower() == self.user_login.lower():
                self.user_id = u.get('_id', 0)
        # We didn't find the user ID, likely wrong username given
        if not self.user_id:
            self.logger.critical(f'Cannot get user ID, check username\n{data}')
            await self.close()
            exit(0)
        self.logger.debug(f'Got user ID {self.user_id}')

    async def get_stream_data(self) -> dict:
        """Returns some stream data if current user is streaming"""
        url = f'https://api.twitch.tv/kraken/streams/{self.user_id}'
        headers = {'Client-ID': self.client_id, 'Accept': 'application/vnd.twitchtv.v5+json'}
        data = await self.aio_request(url=url, headers=headers)
        # API call didn't work
        if not data:
            await self.close()
            exit(0)
        ret = {}
        if data.get('stream') and data['stream'].get('stream_type') == 'live':
            if time_str := data['stream'].get('created_at'):
                try:
                    ret['time'] = isoparse(time_str)
                    # Convert UTC to local time
                    ret['time'] = ret['time'].replace(tzinfo=timezone.utc).astimezone(tz=None)
                except Exception as e:
                    self.logger.warning(f'Cannot parse time string "{time_str}": {e}')
            ret['preview'] = data['stream']['preview']['medium']
            if 'time' not in ret:
                ret['time'] = datetime.now()
            if data['stream'].get('channel'):
                ret['user_logo'] = data['stream']['channel']['logo']
                if data['stream']['channel'].get('status'):
                    ret['title'] = data['stream']['channel']['status']
            if 'title' not in ret:
                ret['title'] = 'UNKNOWN'
                self.dump_response(data, 'streams')
        return ret

    async def get_hosting_target(self) -> str:
        """Returns who current is hosting, empty string if nobody"""
        url = "https://tmi.twitch.tv/hosts"
        params = {'include_logins': 1, 'host': self.user_id}
        data = await self.aio_request(url=url, params=params)
        if not data:
            return ''
        if not data.get('hosts') or len(data['hosts']) == 0:
            return ''
        return data['hosts'][0].get('target_login', '')

    async def aio_request(self, url, headers=None, params=None) -> dict:
        """Run HTTP GET"""
        caller_name = inspect.stack()[1].function
        data = None
        try:
            async with self.aio_sess.get(url=url, headers=headers, params=params) as resp:
                data = await resp.json()
                if resp.status != 200:
                    data_str = json.dumps(data, indent=2)
                    e = Exception(f'Invalid response {resp.status} {resp.reason}')
                    embed = self.make_embed_error(f'{caller_name} failed', e=e)
                    embed.add_field(name='Data', value=f'```json\n{data_str}\n```', inline=False)
                    await self.send_notification(embed=embed)
                    self.logger.error(f'{e}\n\t{data_str}')
                    self.dump_response(data, caller_name)
                    raise e
        except Exception as e:
            if not data:
                embed = self.make_embed_error(f'{caller_name} failed', e=e)
                await self.send_notification(embed=embed)
                self.logger.error(embed.description)
            data = None
        return data

    async def check_if_live(self):
        """
        1. Get user ID
        2. Check for host
        3. Check for streams
        4. Run streamlink if appropriate
        """
        await self.check_en.wait()
        data = await self.get_stream_data()
        if not data:
            self.logger.debug('%s is offline, retrying in %ds', self.user_login, self.timeout)
            self.loop.create_task(self.timer())
            self.check_en.clear()
            return
        # Check if hosting
        if host_login := await self.get_hosting_target():
            self.logger.info('%s is live but hosting %s, retrying in 10m.', self.user_login, host_login)
            self.loop.create_task(self.timer(600))
            self.check_en.clear()
            return
        self.title = data['title'].replace("/", "")
        self.start_time = data['time']
        self.start_time_str = self.start_time.strftime(cfg.TIME_FMT)
        self.logger.info('%s is live: %s', self.user_login, self.title)
        # --- Send notification ---
        embed = self.make_embed()
        embed.colour = Colour.green()
        if 'preview' in data:
            embed.set_image(url=data['preview'])
        if 'user_logo' in data:
            embed.set_thumbnail(url=data['user_logo'])
        await self.send_notification(embed=embed)
        # --- Send notification ---
        await self.record()

    async def record(self):
        self.check_en.clear()
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.title)
        rec_name = F"{self.start_time_str}_{self.user_login}_{no_space_title}"
        raw_fp = os.path.join(self.dst_path, f'{rec_name}.flv')
        stream_url = F"twitch.tv/{self.user_login}"
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
        # Title without illegal NTFS characters, no extra spaces and no trailing whitespace
        win_title = re.sub(r'(\s{2,}|\s+$|[<>:\"/\\|?*\n]+)', '', self.title)
        conv_name = F"{self.start_time_str}_{win_title}"
        # Send job to encoder
        send_dict = {'src': raw_fp, 'file_name': conv_name, 'user': self.user_login, 'raw': True}
        try:
            await self.send_job(send_dict)
        except Exception as e:
            self.logger.error(f'{e}: src {raw_fp}, file_name {conv_name}, user {self.user_login}')
            embed = self.make_embed_error('Failed to send job to encoder', e=e)
            await self.send_notification(embed=embed)
        self.loop.create_task(self.timer())

    async def send_job(self, job: dict):
        writer = await self.open_conn()
        if cfg.JSON_SERIALIZE:
            writer.write(json.dumps(job).encode('utf-8'))
        else:
            writer.write(pickle.dumps(job))
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        self.logger.info("Socket connection closed")

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
