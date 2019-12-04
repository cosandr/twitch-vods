import asyncio
import logging
import os
import re
import sys
import traceback
import json
from datetime import datetime
from logging.handlers import RotatingFileHandler

from aiohttp import ClientSession

import config as cfg


# Run in shell
# IN= ;OUT= ;ffmpeg -i "${IN}" -c:v libx265 -x265-params crf=23:pools=4 -preset:v medium -c:a aac -y -v info -hide_banner "${OUT}"

FFMPEG_HEVC = 'ffmpeg -i "{0}" -c:v libx265 -x265-params crf=23:pools=4 -preset:v medium -c:a aac -y -v info -hide_banner "{1}"'
FFMPEG_COPY = 'ffmpeg -i "{0}" -err_detect ignore_err -f mp4 -acodec aac -c copy -y -v info -hide_banner "{1}"'
TRANSCODE_FILE = 'transcode.json'
BUSY_FILE = os.path.join(cfg.BUSY, cfg.NAME)

class Check():
    title = ''
    start_time = ''
    aio_sess = None
    ffmpeg_src = []
    ffmpeg_task = None
    kill_flag = False
    trans_running = False

    def __init__(self, loop: asyncio.BaseEventLoop, client_id: str, user: str, timeout: int=120):
        self.client_id = client_id
        self.user = user
        self.timeout = timeout
        self.loop = loop
        self.check_en = asyncio.Event()
        self.check_en.set()
        ### Logger ###
        log_fmt = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
        self.rec_log = logging.getLogger('recorder')
        self.t_log = logging.getLogger('transcode')
        self.rec_log.setLevel(logging.DEBUG)
        self.t_log.setLevel(logging.DEBUG)
        if not os.path.exists('log'):
            os.mkdir('log')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(log_fmt)
        for k, v in {'recorder': self.rec_log, 'transcode': self.t_log}.items():
            fh = RotatingFileHandler(
                filename=f'log/{k}.log',
                maxBytes=1e6, backupCount=3,
                encoding='utf-8', mode='a'
            )
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(log_fmt)
            v.addHandler(fh)
            v.addHandler(ch)
        self.rec_log.info("Twitch recorder started with PID %d", os.getpid())
        # Load persistent transcode file
        if os.path.exists(TRANSCODE_FILE):
            with open(TRANSCODE_FILE, 'r', encoding='utf-8') as fr:
                self.ffmpeg_src = json.load(fr)
            self.rec_log.info(f"{len(self.ffmpeg_src)} pending transcodes loaded.")
        self.ffmpeg_task = self.loop.create_task(self.wait_transcode())
    
    async def init_sess(self):
        self.rec_log.info(F"aiohttp session initialized.")
        self.aio_sess = ClientSession()
    
    async def close_sess(self):
        await self.aio_sess.close()
        self.rec_log.info(F"aiohttp session closed.")
    
    async def timer(self, timeout=None):
        self.unmark_busy()
        if timeout is None:
            timeout = self.timeout
        self.rec_log.debug(F"Starting timer with {timeout}s timeout.")
        await asyncio.sleep(timeout)
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
                        self.rec_log.error(F"Invalid response {resp.status}, retrying in {self.timeout}s.")
                        self.loop.create_task(self.timer())
                        self.check_en.clear()
            except Exception as e:
                self.rec_log.error(F"aiohttp error {str(e)}, retrying in {self.timeout}s.")
                self.loop.create_task(self.timer())
                self.check_en.clear()
            if data and (len(data['data']) != 0) and (data['data'][0]['type'] == 'live'):
                self.title = data['data'][0]['title'].replace("/", "")
                self.start_dt = datetime.now()
                self.rec_log.debug(F"{self.user} is live: {self.title}")
                await self.record()
            else:
                self.rec_log.debug(F"{self.user} is offline, retrying in {self.timeout}s.")
                self.loop.create_task(self.timer())
                self.check_en.clear()

    async def record(self):
        self.check_en.clear()
        self.mark_busy()
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.title)
        start_time_str = self.start_dt.strftime("%y%m%d-%H%M")
        rec_name = F"{start_time_str}_{self.user}_{no_space_title}"
        raw_fp = os.path.join(cfg.RAW, f'{rec_name}.flv')
        stream_url = F"twitch.tv/{self.user}"
        self.rec_log.info(F"Saving raw stream to {raw_fp}")
        cmd = F"streamlink {stream_url} --default-stream best -o {raw_fp} -l info"
        try:
            await self.run_cmd(cmd, self.rec_log)
        except Exception as e:
            self.rec_log.error(F"Recording failed: {str(e)}")
            if not os.path.exists(raw_fp):
                self.rec_log.critical(F"No raw file found {raw_fp}")
                self.loop.create_task(self.timer())
                return
            self.rec_log.info(F"Premature exit but raw file exists.")
        self.post_record(raw_fp)

    def post_record(self, raw_fp: str):
        start_time = self.start_dt.strftime("%y%m%d-%H%M")
        # Title without illegal NTFS characters
        win_title = re.sub(r'[<>:"\/\\|?*\n]+', '', self.title)
        conv_name = F"{start_time}_{win_title}"
        proc_path = os.path.join(cfg.PROC, self.user)
        if not os.path.exists(proc_path):
            os.mkdir(proc_path)
        # Decide if we copy or transcode
        is_btn = re.search(r'by\s*the\s*numbers', self.title, re.IGNORECASE)
        if is_btn:
            proc_fp = os.path.join(proc_path, f'{conv_name}.mp4')
            cmd = FFMPEG_COPY.format(raw_fp, proc_fp)
        else:
            proc_fp = os.path.join(proc_path, f'{conv_name}.mkv')
            cmd = FFMPEG_HEVC.format(raw_fp, proc_fp)
        self.ffmpeg_src.append(
            {
                'src': raw_fp,
                'dst': proc_fp,
                'cmd': cmd,
                'status': 'pending'
            }
        )
        self.loop.create_task(self.timer())

    async def wait_transcode(self):
        try:
            while True:
                if self.kill_flag:
                    break
                self.trans_running = False
                await asyncio.sleep(10)
                # Look for first valid task
                conv_idx = None
                for i in range(len(self.ffmpeg_src)):
                    if self.ffmpeg_src[i]['status'] != 'pending':
                        continue
                    conv_idx = i
                    break
                if conv_idx is None:
                    continue
                self.trans_running = True
                self.mark_busy()
                # Run copy/HEVC encode
                try:
                    self.t_log.info(f"Running: {self.ffmpeg_src[conv_idx]['cmd']}")
                    await self.run_cmd(self.ffmpeg_src[conv_idx]['cmd'], self.t_log)
                    try:
                        self.delete_raw(self.ffmpeg_src[conv_idx]['src'], self.ffmpeg_src[conv_idx]['dst'])
                        del self.ffmpeg_src[conv_idx]
                    except Exception as e:
                        self.ffmpeg_src[conv_idx]['status'] = 'delete-fail'
                except Exception as e:
                    self.t_log.error(f"Transcode failed: {str(e)}")
                    self.ffmpeg_src[conv_idx]['status'] = 'transcode-fail'
        except asyncio.CancelledError:
            self.t_log.info(f"Transcode task cancelled")

        with open(TRANSCODE_FILE, 'w', encoding='utf-8') as fw:
            json.dump(self.ffmpeg_src, fw, indent=1, ensure_ascii=False)
            self.t_log.info(f"{len(self.ffmpeg_src)} pending transcodes written.")

    def delete_raw(self, raw_fp: str, proc_fp: str):
        if not os.path.exists(proc_fp):
            self.t_log.critical(F"Raw: {raw_fp}\nProcessed [MISSING]: {proc_fp}")
            return
        raw_size = os.path.getsize(raw_fp)
        proc_size = os.path.getsize(proc_fp)
        raw_size_str = f'{raw_size/1e6:,.1f}MB'
        proc_size_str = f'{proc_size/1e6:,.1f}MB'
        # Make sure MP4 size is at least 40% FLV's
        if proc_size / raw_size < 0.4:
            self.t_log.error(f'Raw: {raw_size_str}\nProcessed [SMALL]: {proc_size_str}')
        else:
            try:
                os.unlink(raw_fp)
                self.t_log.info(f'Raw: {raw_fp} [{raw_size_str}]\nDeleted.')
            except Exception as e:
                self.t_log.warning(f'Raw: {raw_fp} [{raw_size_str}]\nFailed to delete: {str(e)}.')

    async def run_cmd(self, cmd: str, logger: logging.Logger):
        exec_name = cmd.split(' ')[0]
        p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            await asyncio.gather(self.watch(p.stdout, exec_name, logger, prefix='STDOUT:'), self.watch(p.stderr, exec_name, logger, prefix='STDERR:'))
        except Exception as e:
            logger.critical(f'stdout/err critical failure: {str(e)}')
        await p.wait()
        if p.returncode != 0:
            raise Exception(F"{exec_name} exit code: {p.returncode}")

    async def watch(self, stream, proc, logger, prefix=''):
        async for line in stream:
            tmp = line.decode()
            if 'Opening stream' in tmp:
                logger.info(F"Stream opened.")
            elif 'Stream ended' in tmp:
                logger.info(F"Stream ended.")
            elif prefix == 'STDERR:':
                logger.warning(F"[{proc}] {prefix} {tmp}")
            else:
                logger.debug(F"[{proc}] {prefix} {tmp}")

    def mark_busy(self):
        if not os.path.exists(cfg.BUSY):
            return
        if not os.path.exists(BUSY_FILE):
            try:
                open(BUSY_FILE, 'w').close()
            except Exception as e:
                self.rec_log.error(f'Cannot create busy file: {str(e)}')
    
    def unmark_busy(self):
        if not os.path.exists(cfg.BUSY):
            return
        if os.path.exists(BUSY_FILE) and not self.trans_running and not self.check_en.is_set():
            try:
                os.unlink(BUSY_FILE)
            except Exception as e:
                self.rec_log.error(f'Cannot remove busy file: {str(e)}')

if __name__ == '__main__':
    user = os.getenv('STREAM_USER')
    if not user:
        print('A user to check is required')
        sys.exit(0)
    timeout = os.getenv('CHECK_TIMEOUT', '120')
    try:
        timeout = int(timeout)
    except ValueError:
        print('Invalid timeout')
        sys.exit(0)
    loop = asyncio.get_event_loop()
    c = Check(loop, cfg.TWITCH_ID, user=user, timeout=timeout)
    loop.run_until_complete(c.init_sess())
    while True:
        try:
            loop.run_until_complete(c.check_if_live())
        except KeyboardInterrupt:
            print(F"Keyboard interrupt, exit.")
            c.kill_flag = True
            break
        except Exception as error:
            traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
            pass
    loop.run_until_complete(c.close_sess())
