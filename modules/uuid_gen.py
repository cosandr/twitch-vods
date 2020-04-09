import asyncio
import hashlib
import logging
import os
import re
import signal
import uuid
from datetime import datetime, timedelta
from typing import Dict

import asyncpg

import config as cfg
from utils import setup_logger

"""
TODO:
- Socket connection to rec.py
"""


class Generator:
    psql_table_name = 'uuids'
    psql_table = f"""
        CREATE TABLE IF NOT EXISTS {psql_table_name} (
            type      VARCHAR(20) NOT NULL,
            filename  TEXT NOT NULL,
            uuid      UUID NOT NULL,
            md5       UUID NOT NULL UNIQUE,
            created   TIMESTAMP NOT NULL DEFAULT NOW(),
            updated   TIMESTAMP NULL
        );
        CREATE OR REPLACE FUNCTION update_{psql_table_name}_time()
            RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        DROP TRIGGER IF EXISTS trigger_update_{psql_table_name}_time ON {psql_table_name};
        CREATE TRIGGER trigger_update_{psql_table_name}_time BEFORE update ON {psql_table_name}
        FOR EACH ROW EXECUTE PROCEDURE update_{psql_table_name}_time();
        GRANT SELECT ON {psql_table_name} TO discord;
    """

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.uuid_dict = {}
        # Postgres connection string <user>:<pass>@<host>:<port>/<db>
        self.pg_uri: str = ''
        self.conn: asyncpg.Connection = None
        self.src_paths: Dict[str, str] = {}
        self.dst_path: str = ''
        # --- Logger ---
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'uuid')
        # --- Logger ---
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.get_env()
        self.loop.run_until_complete(self.async_init())
        self.logger.info("Generator started with PID %d", os.getpid())

    def signal_handler(self, signalnum, frame):
        raise KeyboardInterrupt()

    def get_env(self):
        self.pg_uri = os.getenv('PG_URI')
        self.dst_path = os.getenv('PATH_DST')
        for k, v in os.environ.items():
            if k.startswith('PATH_SRC'):
                # Source folder format `[TYPE]PATH`
                if m := re.match(r'\[(?P<type>\w+)\](?P<path>.+)', v):
                    if not os.path.exists(m.group('path')):
                        self.logger.critical('Source directory cannot be found: %s', v)
                        exit(0)
                    self.src_paths[m.group('type')] = m.group('path')
        if not self.dst_path:
            self.logger.critical('Missing PATH_DST variable')
            exit(0)
        if not self.src_paths:
            self.logger.critical('Missing PATH_SRC variable(s)')
            exit(0)
        if not self.pg_uri:
            self.logger.critical('Missing PG_URI Postgres connection URI')
            exit(0)

    async def async_init(self):
        self.conn = await asyncpg.connect(dsn=f'postgres://{self.pg_uri}')
        # Check PSQL table
        result = await self.conn.fetchval('SELECT to_regclass($1)', self.psql_table_name)
        if not result:
            await self.conn.execute(self.psql_table)
            self.logger.info('PSQL table %s created', self.psql_table_name)
        else:
            self.logger.info('PSQL table %s OK', self.psql_table_name)

    async def close(self):
        await self.conn.close(timeout=30)
        self.logger.info('PSQL connection closed')

    async def check_new_files(self, wait_time: int = 60):
        """Checks for new files every wait_time seconds"""
        new_file_check = datetime.now()
        replace_all = datetime.now() + timedelta(days=3)
        while True:
            if datetime.now() > new_file_check:
                self.logger.info("Checking for new files.")
                await self.add_new_delete_old()
                new_file_check = datetime.now() + timedelta(minutes=30)
                self.logger.info("Next check: %s", new_file_check.strftime("%Y-%m-%d %H:%M"))
            if datetime.now() > replace_all:
                self.logger.info("Replacing all links.")
                await self.replace_all()
                replace_all = datetime.now() + timedelta(days=3)
                self.logger.info("Next replacement: %s", replace_all.strftime("%Y-%m-%d %H:%M"))
            await asyncio.sleep(wait_time)

    async def replace_all(self):
        # Clear existing links
        for file in os.listdir(self.dst_path):
            os.unlink(os.path.join(self.dst_path, file))

        self.logger.info("Old links deleted")
        # Create links
        for k, v in self.src_paths.items():
            self.uuid_dict[k] = []
            for file in os.listdir(v):
                if file.endswith('.mp4'):
                    tmp_uuid = uuid.uuid1()
                    tmp_link = os.path.join(self.dst_path, tmp_uuid.hex)
                    os.symlink(os.path.join(v, file), tmp_link)
                    created_dt = self.get_datetime(v, file)
                    md5 = self.get_md5(os.path.join(v, file))
                    self.uuid_dict[k].append({"filename": file[:-4], "uuid": tmp_uuid, "created": created_dt, "md5": md5})
            self.logger.info("%s links created", k.upper())
        # Insert/update table.
        async with self.conn.transaction():
            for vid_type, dict_list in self.uuid_dict.items():
                for item in dict_list:
                    q = (f"INSERT INTO {self.psql_table_name} (type, filename, uuid, md5, created) "
                         "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (md5) DO UPDATE SET filename=$2, uuid=$3")
                    await self.conn.execute(q, vid_type, item['filename'], item['uuid'], item['md5'], item['created'])
        self.logger.info("Links added to SQL table")
        # Delete entries which do not exist anymore.
        await self.delete_old_links()

    async def add_new_delete_old(self):
        for k, v in self.src_paths.items():
            for file in os.listdir(v):
                if file.endswith('.mp4'):
                    created_dt = self.get_datetime(v, file)
                    ten_min_ago = datetime.today() - timedelta(minutes=10)
                    if created_dt > ten_min_ago:
                        tmp_uuid = uuid.uuid1().hex
                        tmp_link = os.path.join(self.dst_path, tmp_uuid)
                        os.symlink(os.path.join(v, file), tmp_link)
                        md5 = self.get_md5(os.path.join(v, file))
                        async with self.conn.transaction():
                            q = (f"INSERT INTO {self.psql_table_name} (type, filename, uuid, md5, created) "
                                 "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (md5) DO UPDATE SET filename=$2")
                            await self.conn.execute(q, k, file[:-4], tmp_uuid, md5, created_dt)
                        self.logger.info("New %s %s added.", k.upper(), file[:-4])
                        self.uuid_dict[k].append({"filename": file[:-4], "uuid": tmp_uuid, "created": created_dt, "md5": md5})
        # Delete entries which do not exist anymore.
        await self.delete_old_links()

    async def delete_old_links(self):
        existing_links = set()
        for v in self.uuid_dict.values():
            for entry in v:
                existing_links.add(entry['uuid'])

        async with self.conn.transaction():
            q = f"SELECT uuid FROM {self.psql_table_name}"
            result = await self.conn.fetch(q)
            for entry in result:
                tmp = entry['uuid']
                if tmp not in existing_links:
                    q = f"DELETE FROM {self.psql_table_name} WHERE uuid=$1"
                    await self.conn.execute(q, tmp)
                    self.logger.info("Old link %s removed from table", tmp)

    def get_datetime(self, path, file):
        if m := re.search(r'\d{6}-\d{4}', file):
            return datetime.strptime(m.group(), cfg.TIME_FMT)
        self.logger.debug("%s doesn't contain timestamp, fallback to OS modified time.", file)
        return datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file)))

    @staticmethod
    def get_md5(file):
        md5 = hashlib.md5()
        with open(file, 'rb') as f:
            data = f.read(65536)
            md5.update(data)
        return md5.hexdigest()
