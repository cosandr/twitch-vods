import asyncio
import hashlib
import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import asyncpg

"""
TODO:
- Postgres is hardcoded
- Paths are hardcoded
- Socket connection to rec.py
"""


class Generator:
    def __init__(self, www_path, clip_path, rl_path):
        self.uuid_dict = {}
        self.conn: asyncpg.Connection = None
        self.link_path = www_path
        self.check_paths = {"clips": clip_path, "rl": rl_path}
        self.logger = logging.getLogger('uuid')
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists('log'):
            os.mkdir('log')
        fh = RotatingFileHandler(
            filename=f'log/uuid.log',
            maxBytes=int(1e6), backupCount=3,
            encoding='utf-8', mode='a'
        )
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.logger.info("Generator started with PID %d", os.getpid())

    async def connect_psql(self):
        self.conn = await asyncpg.connect(dsn="postgres://uuid_gen:penis@localdocker:5432/web")
        self.logger.info("PSQL connected.")

    async def check_new_files(self):
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
            await asyncio.sleep(60)

    async def replace_all(self):
        # Clear existing links
        for file in os.listdir(self.link_path):
            os.unlink(os.path.join(self.link_path, file))

        self.logger.info("Old links deleted")
        # Create links
        for k, v in self.check_paths.items():
            self.uuid_dict[k] = []
            for file in os.listdir(v):
                if file.endswith('.mp4'):
                    tmp_uuid = uuid.uuid1().hex
                    tmp_link = os.path.join(self.link_path, tmp_uuid)
                    os.symlink(os.path.join(v, file), tmp_link)
                    created_dt = self.get_datetime(v, file)
                    md5 = self.get_md5(os.path.join(v, file))
                    self.uuid_dict[k].append({"filename": file[:-4], "uuid": tmp_uuid, "created": created_dt, "md5": md5})
            self.logger.info("%s links created", k.upper())
        # Insert/update table.
        async with self.conn.transaction():
            for vid_type, dict_list in self.uuid_dict.items():
                for item in dict_list:
                    q = ("INSERT INTO uuids (type, filename, uuid, md5, created) VALUES ($1, $2, $3, $4, $5)"
                         "ON CONFLICT (md5) DO UPDATE SET filename=$2, uuid=$3")
                    await self.conn.execute(q, vid_type, item['filename'], item['uuid'], item['md5'], item['created'])
        self.logger.info("Links added to SQL table")
        # Delete entries which do not exist anymore.
        await self.delete_old_links()

    async def add_new_delete_old(self):
        for k, v in self.check_paths.items():
            for file in os.listdir(v):
                if file.endswith('.mp4'):
                    created_dt = self.get_datetime(v, file)
                    ten_min_ago = datetime.today() - timedelta(minutes=10)
                    if created_dt > ten_min_ago:
                        tmp_uuid = uuid.uuid1().hex
                        tmp_link = os.path.join(self.link_path, tmp_uuid)
                        os.symlink(os.path.join(v, file), tmp_link)
                        md5 = self.get_md5(os.path.join(v, file))
                        async with self.conn.transaction():
                            q = ("INSERT INTO uuids (type, filename, uuid, md5, created) VALUES ($1, $2, $3, $4, $5)"
                                 "ON CONFLICT (md5) DO UPDATE SET filename=$2")
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
            q = "SELECT uuid FROM uuids"
            result = await self.conn.fetch(q)
            for entry in result:
                tmp = entry['uuid']
                if tmp not in existing_links:
                    q = "DELETE FROM uuids WHERE uuid=$1"
                    await self.conn.execute(q, tmp)
                    self.logger.info("Old link %s removed from table", tmp)

    def get_datetime(self, path, file):
        # match = re.search(r'\d{2}\-\d{2}\-\d{2}\_\d{4}', file)
        match = re.search(r'\d{6}-\d{4}', file)
        if match is None:
            self.logger.debug(F"{file} doesn't contain timestamp, fallback to OS modified time.")
            return datetime.utcfromtimestamp(os.path.getmtime(os.path.join(path, file)))
        # return datetime.strptime(match.group(), "%d-%m-%y_%H%M")
        return datetime.strptime(match.group(), "%y%m%d-%H%M")

    def rename_rl(self):
        for file in os.listdir(self.check_paths['rl']):
            if file.endswith('.mp4'):
                match = re.search(r'\d{2}\-\d{2}\-\d{2}\_\d{4}', file)
                if match is not None:
                    dt = datetime.strptime(match.group(), "%d-%m-%y_%H%M")
                    new_time = dt.strftime("%y%m%d-%H%M")
                    file_title = file.split(F"_{match.group()}", 1)[0]
                    new_name = F"{new_time}_{file_title}.mp4"
                    os.rename(os.path.join(self.check_paths['rl'], file),
                              os.path.join(self.check_paths['rl'], new_name))

    @staticmethod
    def get_md5(file):
        md5 = hashlib.md5()
        with open(file, 'rb') as f:
            data = f.read(65536)
            md5.update(data)

        return md5.hexdigest()


if __name__ == "__main__":
    www_path, clip_path, rl_path = os.getenv('PATH_WWW'), os.getenv('PATH_SRC1'), os.getenv('PATH_SRC2')
    if not www_path or not clip_path or not rl_path:
        print(f'Missing env variables.')
        exit(1)
    gen = Generator(www_path, clip_path, rl_path)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gen.connect_psql())
    loop.run_until_complete(gen.replace_all())
    loop.run_until_complete(gen.check_new_files())
