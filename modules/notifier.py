import asyncio
import json
import logging
import os
from datetime import datetime

from utils import setup_logger


class Notifier:
    def __init__(self, loop: asyncio.AbstractEventLoop, log_parent='', tcp_host=None, tcp_port=None):
        self.loop = loop
        self.tcp_host = '127.0.0.1' if not tcp_host else tcp_host
        self.tcp_port = 6684 if not tcp_port else tcp_port
        # noinspection PyTypeChecker
        self.writer: asyncio.StreamWriter = None
        # noinspection PyTypeChecker
        self.reader: asyncio.StreamReader = None
        # --- Logger ---
        logger_name = self.__class__.__name__
        if log_parent:
            logger_name = f'{log_parent}.{logger_name}'
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'notifier')
        # --- Logger ---
        self.get_env()
        self.loop.run_until_complete(self.async_init())

    def get_env(self):
        tcp_host = os.getenv('TCP_HOST')
        if tcp_host:
            self.tcp_host = tcp_host
        tcp_port = os.getenv('TCP_PORT')
        if tcp_port:
            self.tcp_port = int(tcp_port)

    async def send(self, content: str, name: str = 'Notifier', time: datetime = datetime.now()):
        msg = {
            'name': name,
            'content': content,
            'time': time.isoformat(),
        }
        self.writer.write(json.dumps(msg).encode('utf-8'))
        await self.writer.drain()

    async def async_init(self):
        for i in range(3):
            try:
                self.logger.info("Connecting to TCP server at %s:%d", self.tcp_host, self.tcp_port)
                self.reader, self.writer = await asyncio.open_connection(self.tcp_host, self.tcp_port)
                self.logger.info("Connected to notifications server")
                break
            except Exception as e:
                if i == 2:
                    self.logger.critical("Could not connect to notification server: %s", str(e))
                    raise
                else:
                    self.logger.error("Could not connect to notification server, retrying: %s", str(e))
                    await asyncio.sleep(1)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info("TCP connection closed")
