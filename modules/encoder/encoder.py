import asyncio
import json
import logging
import os
import re
import signal
import time
from datetime import datetime
from typing import List, Optional

from aiohttp import web
from discord import Embed, Colour

from modules import Cleaner, IntroTrimmer, Notifier
from utils import read_video_info, run_ffmpeg, setup_logger, get_datetime
from . import Job, Response

"""
### Run in shell
ffprobe -v quiet -print_format json -show_streams -sexagesimal ${IN}
IN= ;OUT= ;ffmpeg -i "${IN}" -c:v libx265 -x265-params crf=23:pools=4 -preset:v fast -c:a aac -y -hide_banner "${OUT}"
IN= ;OUT= ;ffmpeg -i "${IN}" -c:v libx265 -x265-params crf=23:pools=4 -preset:v faster -c:a aac -y -progress - -nostats -hide_banner "${OUT}"

### OUTPUT OF -progress - -nostats
frame=61
fps=36.08
stream_0_0_q=-0.0
bitrate=   8.6kbits/s
total_size=3050
out_time_us=2838000
out_time_ms=2838000
out_time=00:00:02.838000
dup_frames=0
drop_frames=0
speed=1.68x
progress=continue
"""

NAME = 'Twitch Encoder'
ICON_URL = 'https://raw.githubusercontent.com/cosandr/twitch-vods/master/icons/encoder.png'


# noinspection PyBroadException
class Encoder:
    jobs_file = './data/jobs.json'
    copy_args = [
        '-c:v', 'copy', '-f', 'mp4',
        '-c:a', 'aac',
        '-err_detect', 'ignore_err',
        '-v', 'warning', '-y', '-progress', '-', '-nostats', '-hide_banner'
    ]
    hevc_args = [
        '-c:v', 'libx265', '-x265-params', 'crf=23:pools=4', '-preset:v', 'fast',
        '-c:a', 'aac',
        '-v', 'warning', '-y', '-progress', '-', '-nostats', '-hide_banner'
    ]

    def __init__(self, loop: asyncio.AbstractEventLoop, **kwargs):
        self.loop = loop
        enable_cleaner: bool = kwargs.pop('enable_cleaner', False)
        enable_notifications: bool = not kwargs.get('no_notifications', False)
        self.cleaner: Optional[Cleaner] = kwargs.pop('cleaner', None)
        self.copy_pattern: str = kwargs.pop('copy_pattern', '.*')
        self.copy_pattern_opt: list = kwargs.pop('copy_pattern_opt', [])
        self.dry_run: bool = kwargs.get('dry_run', False)
        self.out_path: str = kwargs.pop('out_path', '.')
        self.hevc_pattern: str = kwargs.pop('hevc_pattern', '')
        self.hevc_pattern_opt: list = kwargs.pop('hevc_pattern_opt', [])
        self.listen_address: str = kwargs.pop('listen_address', '0.0.0.0:3626')
        self.notifier: Optional[Notifier] = kwargs.pop('notifier', None)
        self.print_every: int = kwargs.pop('print_every', 30)
        self.src_path: str = kwargs.pop('src_path', '.')
        self.time_format: str = kwargs.get('time_format', '%y%m%d-%H%M')
        trim_cfg_path: str = kwargs.pop('trim_cfg_path', 'data/trimmer/config.json')
        # --- Logger ---
        logger_name = self.__class__.__name__
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, logger_name.lower())
        kwargs['log_parent'] = logger_name
        # --- Logger ---
        status_str = (
            f'- PID: {os.getpid()}\n'
            f'- Source: {self.src_path}\n'
            f'- Output: {self.out_path}\n'
            f'- File time format: {self.time_format}\n'
            f'- Jobs file: {self.jobs_file}\n'
        )
        if self.dry_run:
            status_str += f'- DRY RUN\n'
        # Removes illegal NTFS characters, extra spaces and trailing whitespace
        self.re_ntfs = re.compile(r'(\s{2,}|\s+$|[<>:\"/\\|?*\n]+)')
        if self.hevc_pattern:
            self.re_hevc = re.compile(self.hevc_pattern, *self.hevc_pattern_opt)
            status_str += f'- HEVC pattern: {self.re_hevc.pattern}\n'
        else:
            self.re_hevc = None
        if self.copy_pattern:
            self.re_copy = re.compile(self.copy_pattern, *self.copy_pattern_opt)
            status_str += f'- Copy pattern: {self.re_copy.pattern}\n'
        else:
            self.re_copy = None
        self.logger.info('\n%s', status_str)
        self.jobs: List[Job] = []
        self.trimmer = IntroTrimmer(cfg_path=trim_cfg_path, **kwargs)
        self.loop.add_signal_handler(signal.SIGTERM, self.signal_handler)
        # Check source and destination directories
        for path in (self.src_path, self.out_path):
            if not os.path.exists(path):
                self.logger.warning('%s does not exist', path)
                if not self.dry_run:
                    os.mkdir(path, 0o750)
                    self.logger.info('%s created', path)

        if enable_notifications:
            if not self.notifier:
                try:
                    self.notifier = Notifier(loop=self.loop, **kwargs)
                except Exception:
                    self.logger.exception('Cannot initialize Notifier')
        else:
            self.logger.info('No notifications')

        if enable_cleaner:
            kwargs['check_path'] = self.src_path
            kwargs['notifier'] = self.notifier
            try:
                self.cleaner = Cleaner(loop=self.loop, **kwargs)
            except Exception:
                self.logger.exception('Cannot initialize Cleaner')
        self.read_jobs()
        embed = self.make_embed()
        embed.colour = Colour.light_grey()
        embed.description = 'Started'
        self.loop.create_task(self.send_notification(embed=embed))

    async def close(self, _app=None):
        """Cleanup"""
        embed = self.make_embed()
        embed.colour = Colour.orange()
        embed.description = 'Closing'
        await self.send_notification(embed=embed)
        self.write_jobs()
        if self.cleaner:
            self.cleaner.close()

    def run_app(self):
        app = web.Application()
        app.on_shutdown.append(self.close)
        routes = [
            web.get("/job/list", self.handler_list),
            web.post("/job/run", self.handler_run),
        ]
        app.add_routes(routes)
        if self.listen_address.startswith('/'):
            web.run_app(app, path=self.listen_address)
        else:
            host, port = self.listen_address.split(':', 1)
            web.run_app(app, host=host, port=int(port))
        # Delete old socket file if needed
        if self.listen_address.startswith('/'):
            os.unlink(self.listen_address)
            self.logger.info(f'Deleted socket file {self.listen_address}')

    @staticmethod
    def make_embed() -> Embed:
        return Embed().set_author(name=NAME, icon_url=ICON_URL)

    def make_embed_error(self, description: str, e=None) -> Embed:
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
        except Exception:
            self.logger.exception('Cannot send notification')

    def update_cleaner(self) -> None:
        """Sets flag for cleaner to update its file list"""
        if not self.cleaner:
            return
        self.cleaner.en_del.set()

    def signal_handler(self):
        self.loop.run_until_complete(self.close())
        exit(0)

    def read_jobs(self):
        if not os.path.exists(self.jobs_file):
            return
        with open(self.jobs_file, 'r', encoding='utf-8') as fr:
            jobs = json.load(fr)
        self.jobs = []
        for j in jobs:
            self.jobs.append(Job.from_dict(j))
        self.logger.info('Jobs file read')

    def write_jobs(self):
        if not self.dry_run:
            jobs = [j.to_dict(no_dt=True) for j in self.jobs]
            with open(self.jobs_file, 'w', encoding='utf-8') as fw:
                json.dump(jobs, fw, indent=2)
        self.logger.info('Jobs file updated')

    async def handler_list(self, r: web.Request) -> web.Response:
        self.logger.debug(r.path)
        resp = Response()
        resp.data = [j.to_dict(no_dt=True) for j in self.jobs]
        return resp.web_response

    async def run_job(self, job: Job):
        # Create folder from username if needed
        if not job.created_at:
            job.created_at = get_datetime(job.input, self.time_format, self.src_path)
        file_name = f'{job.created_at.strftime(self.time_format)}_{self.re_ntfs.sub("", job.title)}'
        will_hevc = bool(self.re_hevc.search(file_name)) if self.re_hevc else False
        will_copy = bool(self.re_copy.search(file_name)) if self.re_copy else False
        self.logger.debug("File: %s, HEVC? %s, Copy? %s", file_name, str(will_hevc), str(will_copy))
        if will_hevc:
            cmd = self.hevc_args.copy()
            job.out_file = file_name + '.mkv'
            job.enc_codec = 'hevc'
        elif will_copy:
            cmd = self.copy_args.copy()
            job.out_file = file_name + '.mp4'
            job.enc_codec = 'copy'
        else:
            embed = self.make_embed()
            embed.title = 'Ignore'
            embed.description = job.title
            await self.send_notification(embed=embed)
            self.logger.info(f'Ignoring job for {job.input}')
            job.ignore = True
            self.write_jobs()
            self.update_cleaner()
            return
        in_fp = os.path.join(self.src_path, job.input)
        tmp_out_fp = os.path.join(self.src_path, f'enc_{job.input}')
        # Insert input
        cmd.insert(0, '-i')
        cmd.insert(1, in_fp)
        # Append output file
        cmd.append(tmp_out_fp)
        self.logger.info(f"Encoding {job.input} -> {job.out_file}")
        embed = self.make_embed()
        embed.title = 'Encode'
        embed.add_field(name='Source', value=job.input, inline=True)
        embed.add_field(name='Target', value=job.out_file, inline=True)
        # Try to encode
        try:
            job.enc_cmd = ' '.join(cmd)
            job.enc_start = datetime.utcnow().isoformat()
            if not self.dry_run:
                await run_ffmpeg(logger=self.logger, args=cmd, print_every=self.print_every)
            job.enc_end = datetime.utcnow().isoformat()
            td = datetime.fromisoformat(job.enc_end) - datetime.fromisoformat(job.enc_start)
            status = f"Encoded {job.input} in {str(td)}"
            self.logger.info(status)
            embed.add_field(name='Encode Time', value=str(td), inline=False)
            job.raw = False
        except Exception as e:
            embed.add_field(name='Encode Failed', value=str(e), inline=False)
            self.logger.exception('Encoding failed')
            job.error = str(e)
            job.ignore = True
        # Trim intro if we can
        if not job.error and self.trimmer.get_cfg(job.title):
            try:
                trim_fp = os.path.join(self.src_path, f'trimmed_{job.input}')
                intro_seconds = await self.trimmer.run_crop(file=tmp_out_fp, out_file=trim_fp, check_name=job.title)
                embed.add_field(name='Trimmed', value=f'{intro_seconds} seconds', inline=False)
                job.start_seconds = intro_seconds
                # Replace encoded file with trimmed one
                os.replace(trim_fp, tmp_out_fp)
            except Exception as e:
                self.logger.exception('Could not find intro seconds')
                embed.add_field(name='Trim Failed', value=str(e), inline=False)
        # Try to delete raw
        if not job.error and not job.start_seconds:
            try:
                await self.delete_raw(in_fp, tmp_out_fp)
                embed.set_field_at(0, name='Source Deleted', value=job.input, inline=True)
            except Exception as e:
                job.error = str(e)
                embed.add_field(name='Source Delete Failed', value=str(e), inline=False)
        # Move encoded file to final directory
        if os.path.exists(tmp_out_fp):
            try:
                out_path = os.path.join(self.out_path, job.user)
                if not os.path.exists(out_path):
                    os.mkdir(out_path, 0o750)
                out_fp = os.path.join(out_path, job.out_file)
                os.replace(tmp_out_fp, out_fp)
            except Exception as e:
                job.error = str(e)
                embed.add_field(name='Move Failed', value=str(e), inline=False)
        await self.send_notification(embed=embed)
        job.deleted = not os.path.exists(in_fp)
        self.write_jobs()
        self.update_cleaner()

    async def handler_run(self, r: web.Request) -> web.Response:
        self.logger.debug(r.path)
        immediate = 'immediate' in r.query
        resp = Response()
        job_json: dict = await r.json()
        try:
            self.jobs.append(Job.from_dict(job_json))
            job = self.jobs[-1]
        except Exception as e:
            resp.error = str(e)
            resp.status = web.HTTPBadRequest.status_code
            return resp.web_response
        self.logger.debug('Got job from %s\n%s', r.remote, job.to_json(indent=2))
        in_fp = os.path.join(self.src_path, job.input)
        if not os.path.exists(in_fp):
            status = f"Source file not found: {in_fp}"
            embed = self.make_embed_error('Encode failed', e=status)
            await self.send_notification(embed=embed)
            self.logger.error(status)
            job.ignore = True
            self.write_jobs()
            resp.error = status
            resp.status = web.HTTPBadRequest.status_code
            return resp.web_response
        if immediate:
            self.loop.create_task(self.run_job(job))
            resp.data = "Job started"
            return resp.web_response
        start = time.perf_counter()
        await self.run_job(job)
        resp.time = time.perf_counter() - start
        resp.data = "Job done"
        return resp.web_response

    async def delete_raw(self, raw_fp: str, proc_fp: str):
        if not os.path.exists(proc_fp):
            self.logger.critical('%s -> MISSING %s', raw_fp, proc_fp)
            raise FileNotFoundError(proc_fp)
        raw_size = os.path.getsize(raw_fp)
        proc_size = os.path.getsize(proc_fp)
        raw_size_str = f'{raw_size/1e6:,.1f}MB'
        proc_size_str = f'{proc_size/1e6:,.1f}MB'
        raw_dur = await read_video_info(raw_fp, self.logger)
        proc_dur = await read_video_info(proc_fp, self.logger)
        if raw_dur is None:
            self.logger.warning('Cannot parse duration: %s', raw_fp)
            raise Exception('Cannot parse raw duration')
        elif proc_dur is None:
            self.logger.warning('Cannot parse duration: %s', proc_fp)
            raise Exception('Cannot parse processed duration')
        dur_diff = raw_dur - proc_dur
        if dur_diff.total_seconds() > 5:
            self.logger.warning('%s [%s] -> SHORTER %s [%s]', raw_dur, raw_size_str, proc_dur, proc_size_str)
            raise Exception(f'{proc_fp} is too short: {proc_dur}')
        try:
            if not self.dry_run:
                os.unlink(raw_fp)
            self.logger.info('Deleted: %s [%s]', raw_fp, raw_size_str)
        except Exception as e:
            self.logger.error('Failed to delete %s [%s]: %s', raw_fp, raw_size_str, str(e))
            raise
