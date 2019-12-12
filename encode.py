#!/usr/bin/python3

import asyncio
import json
import logging
import os
import pickle
import re
import signal
import traceback
from logging.handlers import RotatingFileHandler

import config as cfg
from utils import parse_duration, read_video_info

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

class JobRecv():

    """
    JOB = {
        src: path to source file
        file_name: file name with no extension/path
        user: streamer username
    }
    """

    server = None
    job_keys = ['src', 'file_name', 'user']

    def __init__(self, loop, logger, jobs: list, jobs_done:list, en: asyncio.Event):
        self.loop = loop
        self.logger = logger
        self.jobs = jobs
        self.jobs_done = jobs_done
        self.en = en

    async def run(self):
        """Start deserialize"""
        self.server = await asyncio.start_unix_server(self.deserialize, path=cfg.SOCK, loop=self.loop)
        self.logger.info(F"Socket at {self.server.sockets[0].getsockname()} started")

    async def close(self):
        """Close server and remove socket file"""
        self.server.close()
        self.logger.info(F"Socket closed")
        await self.server.wait_closed()
        os.unlink(cfg.SOCK)

    async def deserialize(self, reader, writer):
        """Get and check UNIX socket messages"""
        data = await reader.read(1024)
        msg = pickle.loads(data)
        self.logger.debug(f"Message received: {msg}")
        # Check message type
        if not isinstance(msg, dict):
            err = f'Expected dict, got {type(msg)}'
            self.logger.error(err)
            self.jobs_done.append({'name': msg, 'status': err})
            return
        # Make sure we have all required keys
        for k in self.job_keys:
            if k not in msg.keys():
                err = f'Key {k} is missing'
                self.logger.error(err)
                self.jobs_done.append({'name': msg, 'status': err})
                return
        # Check paths
        if not os.path.exists(msg['src']):
            err = f'Source file not found'
            self.logger.error(err)
            self.jobs_done.append({'name': msg, 'status': err})
            return
        self.jobs.append(msg)
        self.en.set()


class Encoder():
    
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

    def __init__(self, logger, jobs: list, jobs_done: list, en: asyncio.Event, dst_path: str='.'):
        self.logger = logger
        self.jobs = jobs
        self.jobs_done = jobs_done
        self.en = en
        self.dst_path = dst_path
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signal, frame):
        raise KeyboardInterrupt()
    
    async def job_wait(self):
        await self.en.wait()
        # Clear if this is the last job
        if len(self.jobs) == 1:
            self.en.clear()
        j = self.jobs[-1]
        # Create folder from username if needed
        out_path = os.path.join(self.dst_path, j['user'])
        if not os.path.exists(out_path):
            os.mkdir(out_path)
        # Encode to HEVC if not BTN episode
        is_btn = re.search(r'by\s*the\s*numbers', j['file_name'], re.IGNORECASE)
        if is_btn:
            cmd = self.copy_args.copy()
            out_fp = os.path.join(out_path, j['file_name']+'.mp4')
        else:
            cmd = self.hevc_args.copy()
            out_fp = os.path.join(out_path, j['file_name']+'.mkv')
        # Insert input
        cmd.insert(0, '-i')
        cmd.insert(1, j['src'])
        # Append output file
        cmd.append(out_fp)
        # Try to encode
        try:
            self.logger.info('Encoding %s -> %s', j['src'], out_fp)
            await self.run(cmd)
            done_status = f'Encoded: {out_fp}'
            self.logger.info(done_status)
        except Exception as e:
            done_status = f'Encoding failed: {str(e)}'
            self.logger.error(done_status)
        # Try to delete raw
        try:
            await self.delete_raw(j['src'], out_fp)
            done_status = f'Deleted: {j["src"]}'
        except Exception as e:
            done_status = f'Delete failed: {type(e)} {str(e)}'
        self.jobs_done.append({'name': j['file_name'], 'status': done_status})
        self.jobs.pop()

    async def run(self, args: list):
        self.logger.debug('CMD: ffmpeg %s', ' '.join(args))
        p = await asyncio.create_subprocess_exec('ffmpeg', *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            await asyncio.gather(self.watch(p.stdout, prefix='STDOUT:'), self.watch(p.stderr, prefix='STDERR:'))
        except Exception as e:
            self.logger.critical(f'stdout/err critical failure: {str(e)}')
        await p.wait()
        if p.returncode != 0:
            raise Exception(F"ffmpeg exit code: {p.returncode}")

    async def watch(self, stream: asyncio.StreamReader, prefix=''):
        print_every = 10
        last_time = 0
        log_dict = {}
        try:
            async for line in stream:
                tmp = line.decode()
                # Parse output
                parsed = {
                    'frame': tmp.split('frame='),
                    'fps': tmp.split('fps='),
                    'size': tmp.split('total_size='),
                    'out_time': tmp.split('out_time=')
                }
                # Add found value to log_dict
                for k, v in parsed.items():
                    if len(v) > 1:
                        try:
                            log_dict[k] = float(v[1])
                        except ValueError:
                            log_dict[k] = v[1].replace('\n', '')
                        break
                if log_dict.keys() == parsed.keys():
                    # Log every print_every seconds
                    curr_time = parse_duration(log_dict['out_time'])
                    if curr_time is not None:
                        curr_time = curr_time.total_seconds()
                    if (curr_time is None or abs(curr_time - last_time) >= print_every):
                        self.logger.debug('frame %.0f, FPS %.2f, time %s, size %.1fMB',
                            log_dict["frame"], log_dict["fps"], str(log_dict["out_time"]),
                            log_dict["size"]/1e6)
                        log_dict.clear()
                        last_time = curr_time
        except ValueError as e:
            self.logger.warning('[ffmpeg] Stream Error: %s', str(e))
            pass
    
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
        if dur_diff.total_seconds() > 2:
            self.logger.warning('%s [%s] -> SHORTER %s [%s]', raw_dur, raw_size_str, proc_dur, proc_size_str)
            raise Exception(f'{proc_fp} is too short: {proc_dur}')
        try:
            os.unlink(raw_fp)
            self.logger.info('Deleted: %s [%s]', raw_fp, raw_size_str)
        except Exception as e:
            self.logger.error('Failed to delete %s [%s]: %s', raw_fp, raw_size_str, str(e))
            raise


def read_jobs(fp: str):
    jobs = []
    if not os.path.exists(fp):
        return jobs
    with open(fp, 'r', encoding='utf-8') as fr:
        jobs = json.load(fr)
    return jobs


def write_jobs(fp: str, jobs: list):
    with open(fp, 'w', encoding='utf-8') as fw:
        json.dump(jobs, fw, indent=1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    ### LOGGER ###
    log_fmt = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    logger = logging.getLogger('encoder')
    recv_logger = logging.getLogger('jobrecv')
    logger.setLevel(logging.DEBUG)
    recv_logger.setLevel(logging.DEBUG)
    if not os.path.exists('log'):
        os.mkdir('log')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(log_fmt)
    fh = RotatingFileHandler(
        filename=f'log/encoder.log',
        maxBytes=1e6, backupCount=3,
        encoding='utf-8', mode='a'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(log_fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    recv_logger.addHandler(fh)
    recv_logger.addHandler(ch)
    logger.info("Encoder started with PID %d", os.getpid())
    ### Check destination path ###
    dst_path = '.'
    env_path = os.getenv('PATH_PROC')
    if env_path:
        if not os.path.exists(env_path):
            logger.warning('%s does not exist', env_path)
        else:
            dst_path = env_path
    logger.info('Saving encoded files to %s', dst_path)
    ### Read pending files ###
    jobs = read_jobs('pending_jobs.json')
    jobs_done = read_jobs('jobs_done.json')
    ### Setup variables ###
    en = asyncio.Event()
    recv = JobRecv(loop, recv_logger, jobs, jobs_done, en)
    encoder = Encoder(logger, jobs, jobs_done, en, dst_path)
    # Start receiver
    loop.run_until_complete(recv.run())
    # Set flag if we found jobs in file
    if len(jobs) > 0:
        encoder.en.set()
    while True:
        try:
            loop.run_until_complete(encoder.job_wait())
        except KeyboardInterrupt:
            print("Keyboard interrupt, exit.")
            break
        except Exception as error:
            traceback.print_exception(type(error), error, error.__traceback__)
            pass
    # Stop server
    loop.run_until_complete(recv.close())
    # Write files at the end
    write_jobs('pending_jobs.json', encoder.jobs)
    write_jobs('jobs_done.json', encoder.jobs_done)
