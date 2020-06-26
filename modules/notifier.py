import asyncio
import json
import logging
import os
from datetime import datetime

from aiohttp import ClientSession
from discord import Webhook, AsyncWebhookAdapter, Embed

from utils import setup_logger

TIME_FORMAT = '%H:%M:%S'
EMBED_COLOUR = 0x36393E  # "Transparent" when using dark theme
USERNAME = 'Twitch'
ICON_URL = 'https://www.dresrv.com/icons/twitch-webhook.png'


class Notifier:
    def __init__(self, loop: asyncio.AbstractEventLoop, log_parent='', sess=None, mention_id=None, webhook_url=None):
        self.loop = loop
        self._created_sess = False
        self.sess: ClientSession = sess
        # --- Logger ---
        logger_name = self.__class__.__name__
        if log_parent:
            logger_name = f'{log_parent}.{logger_name}'
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        if not log_parent:
            setup_logger(self.logger, 'notifier')
        # --- Logger ---
        if not webhook_url:
            webhook_url = os.getenv('WEBHOOK_URL')
            if not webhook_url:
                self.logger.critical("WEBHOOK_URL is required")
                raise RuntimeError("webhook_url is required")
        if not self.sess:
            self.loop.run_until_complete(self.async_init())
        self.mention_id = mention_id
        self.webhook = Webhook.from_url(webhook_url, adapter=AsyncWebhookAdapter(self.sess))

    async def async_init(self):
        self.logger.info("aiohttp session initialized")
        self.sess = ClientSession()
        self._created_sess = True

    async def close(self):
        if self._created_sess:
            await self.sess.close()
            self.logger.info("aiohttp session closed")

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
