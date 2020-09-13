import asyncio
import json
import logging
import os
import re
import signal
from typing import Optional

from aiohttp import ClientSession, UnixConnector
from discord import Embed, Colour

from modules.encoder import Job
from modules.notifier import Notifier
from . import LOGGER, StreamData, InvalidResponseError, UserData

NAME = 'Twitch Recorder'
ICON_URL = 'https://raw.githubusercontent.com/cosandr/twitch-vods/master/icons/recorder.png'


# noinspection PyBroadException
class Recorder:
    dumps_path = 'log/dumps'

    def __init__(self, loop: asyncio.AbstractEventLoop, **kwargs):
        self.loop = loop
        enable_notifications: bool = not kwargs.get('no_notifications', False)
        self.dry_run: bool = kwargs.get('dry_run', False)
        self.enc_path: str = kwargs.pop('enc_path', 'http://127.0.0.1:3626')
        self.notifier: Optional[Notifier] = kwargs.pop('notifier', None)
        self.out_path: str = kwargs.pop('out_path', '.')
        self.time_format: str = kwargs.get('time_format', '%y%m%d-%H%M')
        self.timeout: int = int(kwargs.pop('timeout', 120))
        self.twitch_id: str = kwargs.pop('twitch_id')
        user_login = kwargs.pop('user')
        self.aio_sess: Optional[ClientSession] = None
        self.check_en = asyncio.Event()
        self.ended_ok = False
        self.notifier: Optional[Notifier] = None
        self.stream: Optional[StreamData] = None
        self.unix_sess: Optional[ClientSession] = None
        self.user: Optional[UserData] = None
        self.loop.add_signal_handler(signal.SIGTERM, self.signal_handler)
        # --- Logger ---
        self.logger = logging.getLogger(f'{LOGGER.name}.{user_login}')
        self.logger.setLevel(logging.DEBUG)
        kwargs['log_parent'] = self.logger.name
        # --- Logger ---
        status_str = (
            f'- Encoder: {self.enc_path}\n'
            f'- File time format: {self.time_format}\n'
            f'- Output: {self.out_path}\n'
            f'- PID: {os.getpid()}\n'
            f'- Timeout: {self.timeout}\n'
            f'- Twitch client ID: {self.twitch_id}\n'
            f'- User: {user_login}\n'
        )
        if self.dry_run:
            status_str += f'- DRY RUN\n'
        self.logger.info("\n%s", status_str)
        self.loop.run_until_complete(self.async_init(user_login))

        if enable_notifications:
            if not self.notifier:
                kwargs['sess'] = self.aio_sess
                try:
                    self.notifier = Notifier(loop=self.loop, **kwargs)
                except Exception:
                    self.logger.exception('Cannot initialize Notifier')
        else:
            self.logger.info('No notifications')

        # Check args
        if not os.path.exists(self.out_path):
            self.logger.warning('%s does not exist', self.out_path)
            if not self.dry_run:
                os.mkdir(self.out_path, 0o750)
                self.logger.info('%s created', self.out_path)

        embed = self.make_embed()
        embed.colour = Colour.light_grey()
        embed.description = 'Started'
        self.loop.create_task(self.send_notification(embed=embed))
        self.check_en.set()

    async def async_init(self, user_login):
        self.logger.debug("aiohttp session initialized.")
        self.aio_sess = ClientSession()
        if self.enc_path.startswith('/'):
            self.unix_sess = ClientSession(connector=UnixConnector(path=self.enc_path))
            self.logger.debug("Unix session initialized.")
        self.user = await self.get_user_id(user_login)
        if not self.user:
            self.logger.critical('Cannot find user %s', user_login)
            await self.close()
            raise RuntimeError(f'Cannot find user {user_login}')

    async def close(self):
        embed = self.make_embed()
        embed.colour = Colour.orange()
        embed.description = 'Closing'
        await self.send_notification(embed=embed)
        await self.aio_sess.close()
        self.logger.debug("aiohttp session closed")

    def signal_handler(self):
        self.loop.run_until_complete(self.close())
        exit(0)

    def make_embed(self) -> Embed:
        embed = Embed()
        if self.user:
            embed.title = self.user.display_name
        if self.stream:
            embed.description = f'Recording {self.stream.title}'
        embed.set_author(name=NAME, icon_url=ICON_URL)
        return embed

    def make_embed_error(self, description: str, e: Exception = None) -> Embed:
        embed = self.make_embed()
        embed.colour = Colour.red()
        embed.description = description
        if e:
            embed.add_field(name='Error', value=str(e), inline=False)
        return embed

    async def send_notification(self, content: str = '', embed: Embed = None) -> bool:
        if not self.notifier:
            return False
        try:
            if not embed and content:
                embed = self.make_embed()
                embed.description = content
            await self.notifier.send(embed=embed)
            return True
        except Exception:
            self.logger.exception('Cannot send notification')
            return False

    async def timer(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        await asyncio.sleep(timeout)
        self.check_en.set()

    async def get_user_id(self, user_login: str) -> Optional[UserData]:
        """Sets the user's ID"""
        url = 'https://api.twitch.tv/kraken/users'
        headers = {'Client-ID': self.twitch_id, 'Accept': 'application/vnd.twitchtv.v5+json'}
        params = {'login': user_login}
        try:
            data = await self.http_get_json(url=url, headers=headers, params=params)
        except InvalidResponseError as e:
            self.logger.error('get_user_id: %s\nData: %s', str(e), e.data_str)
            return None
        except Exception:
            self.logger.exception('Failed to get hosting user data')
            return None
        if not data:
            return None
        for d in data.get('users', []):
            u = UserData.from_json(d)
            if u.name.lower() == user_login.lower():
                return u
        return None

    async def get_stream_data(self) -> Optional[StreamData]:
        """Returns some stream data if current user is streaming"""
        url = f'https://api.twitch.tv/kraken/streams/{self.user.id}'
        headers = {'Client-ID': self.twitch_id, 'Accept': 'application/vnd.twitchtv.v5+json'}
        data = await self.http_get_json(url=url, headers=headers)
        ret = StreamData.from_json(data)
        if ret and not ret.title:
            ret.title = 'UNKNOWN'
            self.logger.warning('Stream title missing\n%s', json.dumps(data, indent=2))
        return ret

    async def get_hosting_target(self) -> str:
        """Returns who current is hosting, empty string if nobody"""
        url = "https://tmi.twitch.tv/hosts"
        params = {'include_logins': 1, 'host': self.user.id}
        try:
            data = await self.http_get_json(url=url, params=params)
        except InvalidResponseError as e:
            self.logger.error('get_hosting_target: %s\nData: %s', str(e), e.data_str)
            return ''
        except Exception:
            self.logger.exception('Failed to get hosting target data')
            return ''
        if not data or not data.get('hosts') or len(data['hosts']) == 0:
            return ''
        return data['hosts'][0].get('target_login', '')

    async def http_get_json(self, url, headers=None, params=None) -> dict:
        """Run HTTP GET"""
        async with self.aio_sess.get(url=url, headers=headers, params=params) as resp:
            # This can raise an exception
            data = await resp.json()
            if resp.status != 200:
                raise InvalidResponseError(resp.status, resp.reason, data)
        return data

    async def http_post_data(self, url: str, data: str, **kwargs):
        if self.unix_sess:
            sess = self.unix_sess
            url = f'http://unix{url}'
        else:
            sess = self.aio_sess
            url = f'{self.enc_path}{url}'
        async with sess.post(url, data=data, **kwargs) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise InvalidResponseError(resp.status, resp.reason, data)
        return data

    async def check_if_live(self):
        """
        1. Check for host
        2. Check for streams
        3. Run streamlink if appropriate
        """
        await self.check_en.wait()
        try:
            self.stream = await self.get_stream_data()
        except InvalidResponseError as e:
            self.logger.error('%s\nData: %s', str(e), e.data_str)
            embed = self.make_embed_error('get_stream_data failed', e=e)
            embed.add_field(name='Data', value=f'```json\n{e.data_str}\n```', inline=False)
            await self.send_notification(embed=embed)
            self.loop.create_task(self.timer())
            self.check_en.clear()
            return
        except Exception:
            self.logger.exception('Failed to get stream data')
            self.loop.create_task(self.timer())
            self.check_en.clear()
            return
        if not self.stream:
            self.logger.debug('%s is offline, retrying in %ds', self.user.display_name, self.timeout)
            self.loop.create_task(self.timer())
            self.check_en.clear()
            return
        # Check if hosting
        if host_login := await self.get_hosting_target():
            self.logger.info('%s is live but hosting %s, retrying in 10m.', self.user.display_name, host_login)
            self.loop.create_task(self.timer(600))
            self.check_en.clear()
            return
        self.stream.created_at_str = self.time_format
        self.logger.info('%s is live: %s', self.user.display_name, self.stream.title)
        # --- Send notification ---
        embed = self.make_embed()
        embed.colour = Colour.green()
        if self.stream.preview:
            embed.set_image(url=self.stream.preview)
        if self.stream.user_logo:
            embed.set_thumbnail(url=self.stream.user_logo)
        await self.send_notification(embed=embed)
        # --- Send notification ---
        await self.record()

    async def record(self):
        self.check_en.clear()
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.stream.title)
        rec_name = f"{self.stream.created_at_str}_{self.user.name}_{no_space_title}"
        raw_name = f'{rec_name}.flv'
        raw_fp = os.path.join(self.out_path, raw_name)
        self.logger.info('Saving raw stream to %s', raw_fp)
        if self.dry_run:
            self.logger.info('Dry run, do not run streamlink')
            return
        cmd = f"streamlink {self.stream.url} --default-stream best -o {raw_fp} -l info"
        try:
            await self.run(cmd)
        except Exception as e:
            self.logger.error('Recording failed %s', str(e))
            if not os.path.exists(raw_fp):
                self.logger.critical('No raw file found %s', raw_fp)
                self.loop.create_task(self.timer())
                return
            self.logger.info("Premature exit but raw file exists.")
        await self.post_record(raw_name)

    async def post_record(self, raw_name: str):
        # Send job to encoder
        job = Job(input=raw_name, title=self.stream.title, user=self.user.display_name, created_at=self.stream.created_at)
        job_str = job.to_json(indent=2)
        self.logger.debug("Sending job to encoder\n%s", job_str)
        try:
            await self.http_post_data(url='/job/run', data=job.to_json(), params=dict(immediate='true'))
        except Exception as e:
            self.logger.exception('Failed to send job to encoder')
            embed = self.make_embed_error('Failed to send job to encoder', e=e)
            await self.send_notification(embed=embed)
        self.loop.create_task(self.timer())

    async def run(self, cmd: str):
        p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            # noinspection PyTypeChecker
            await asyncio.gather(self.watch(p.stdout, prefix='STDOUT'), self.watch(p.stderr, prefix='STDERR'))
        except Exception as e:
            self.logger.critical('stdout/err critical failure: %s', str(e))
        await p.wait()
        if p.returncode != 0 and not self.ended_ok:
            raise Exception(f"[streamlink] Non-zero exit code {p.returncode}")
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
