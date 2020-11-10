import asyncio
import logging
from datetime import datetime

from aiohttp import ClientSession
from discord import Webhook, AsyncWebhookAdapter, Embed

TIME_FORMAT = '%H:%M:%S'
EMBED_COLOUR = 0x36393E  # "Transparent" when using dark theme
USERNAME = 'Twitch'
ICON_URL = 'https://raw.githubusercontent.com/cosandr/twitch-vods/874079098145fd99cbe0c41e5120b0e668af79be/icons/webhook.png'


class Notifier:
    def __init__(self, loop: asyncio.AbstractEventLoop, webhook_url: str, **kwargs):
        self.loop = loop
        self.sess: ClientSession = kwargs.get('sess', None)
        self.webhook = None
        self.mention_id: str = kwargs.pop('mention_id', '')
        self._created_sess = False
        # --- Logger ---
        log_parent: str = kwargs.get('log_parent', 'Twitch')
        logger_name = f'{log_parent}.{self.__class__.__name__}'
        self.logger: logging.Logger = logging.getLogger(logger_name)
        # --- Logger ---
        self.init_task = self.loop.create_task(self.async_init(webhook_url))

    async def async_init(self, webhook_url):
        if not self.sess:
            self.sess = ClientSession()
            self._created_sess = True
            self.logger.debug("aiohttp session initialized")
        self.webhook = Webhook.from_url(webhook_url, adapter=AsyncWebhookAdapter(self.sess))
        status_str = f'- Webhook: {webhook_url}\n'
        if self.mention_id:
            status_str += f'- Mention: {self.mention_id}\n'
        self.logger.info("\n%s", status_str)

    async def close(self):
        if self._created_sess:
            await self.sess.close()
            self.logger.debug("aiohttp session closed")

    async def send(self, content: str = '', title: str = '', embed: Embed = None, name: str = 'Notifier', time: datetime = None):
        if not any((content, title, embed)):
            raise RuntimeError('Need one of content, title or embed')
        if self.mention_id:
            msg_content = f'<@{self.mention_id}>'
        else:
            msg_content = ''
        if embed is None:
            embed = Embed(title=title, description=content, colour=EMBED_COLOUR)
            embed.set_author(name=name)
        if time is None:
            time = datetime.now()
        embed.set_footer(text=time.strftime(TIME_FORMAT))
        self.logger.debug(f'Sending message from {name}')
        await self.webhook.send(content=msg_content, embed=embed, username=USERNAME, avatar_url=ICON_URL)

        self.logger.debug(f'Sent message from {name}')
