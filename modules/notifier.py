import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Tuple

from utils import setup_logger


class Notifier:
    def __init__(self, loop: asyncio.AbstractEventLoop, log_parent='', tcp_host=None, tcp_port=None):
        self.loop = loop
        self.tcp_host = '127.0.0.1' if not tcp_host else tcp_host
        self.tcp_port = 6684 if not tcp_port else tcp_port
        # --- Logger ---
        logger_name = self.__class__.__name__
        if log_parent:
            logger_name = f'{log_parent}.{logger_name}'
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'notifier')
        # --- Logger ---
        self.get_env()

    def get_env(self):
        tcp_host = os.getenv('NOTI_HOST')
        if tcp_host:
            self.tcp_host = tcp_host
        tcp_port = os.getenv('NOTI_PORT')
        if tcp_port:
            self.tcp_port = int(tcp_port)

    async def send(self, content: str, name: str = 'Notifier', time: datetime = None):
        try:
            _, writer = await self.open_conn()
        except:
            return
        if time is None:
            time = datetime.now()
        self.logger.info('Sending message from %s', name)
        msg = {
            'name': name,
            'content': content,
            'time': time.isoformat(),
        }
        writer.write(json.dumps(msg).encode('utf-8'))
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        self.logger.info('Message from %s sent', name)

    async def open_conn(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        for i in range(3):
            try:
                self.logger.info("Connecting to TCP server at %s:%d", self.tcp_host, self.tcp_port)
                reader, writer = await asyncio.open_connection(self.tcp_host, self.tcp_port)
                self.logger.info("Connected to notifications server")
                return reader, writer
            except Exception as e:
                if i == 2:
                    self.logger.critical("Could not connect to notification server: %s", str(e))
                    raise
                else:
                    self.logger.error("Could not connect to notification server, retrying: %s", str(e))
                    await asyncio.sleep(1)
