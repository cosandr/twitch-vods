#!/usr/bin/env python3

import argparse
import asyncio
import logging
import os
import re
import traceback
from logging.handlers import RotatingFileHandler
from typing import Dict

from modules import Encoder, Recorder, Generator, Notifier, Cleaner


def merge_env_args(env_map: dict, args: argparse.Namespace) -> dict:
    """Reads environment variables defined in env_map and overrides them with args if needed"""
    kwargs = dict()
    # Read from environment variables
    for k, v in env_map.items():
        if not v:
            continue
        if val := os.getenv(v):
            kwargs[k] = val
    # Read from arguments, overrides env vars
    for k, v in vars(args).items():
        if k in env_map and v is not None:
            if k.endswith('pattern_opt'):
                opts = [getattr(re, name) for name in v]
                kwargs[k] = opts
            else:
                kwargs[k] = v
    return kwargs


def run_cleaner(loop: asyncio.AbstractEventLoop, args: argparse.Namespace):
    env_map = {
        "clean_days": None,
        "no_notifications": None,
        "time_format": "TIME_FORMAT",
        "warn_at": None,
        "webhook_url": "WEBHOOK_URL",
    }
    kwargs = merge_env_args(env_map, args)
    inst = Cleaner(loop, check_path=args.path, **kwargs)

    async def _run():
        await inst.init_task
        await inst.worker_task

    loop.run_until_complete(_run())


def run_encoder(loop: asyncio.AbstractEventLoop, args: argparse.Namespace):
    env_map = {
        "busy_file": "BUSY_FILE",
        "clean_days": None,
        "copy_pattern": None,
        "copy_pattern_opt": None,
        "dry_run": None,
        "enable_cleaner": None,
        "hevc_pattern": None,
        "hevc_pattern_opt": None,
        "listen_address": "ENC_LISTEN_ADDRESS",
        "no_notifications": None,
        "out_path": "ENC_OUT",
        "print_every": None,
        "src_path": "ENC_SRC",
        "time_format": "TIME_FORMAT",
        "warn_at": None,
        "webhook_url": "WEBHOOK_URL",
    }
    kwargs = merge_env_args(env_map, args)
    inst = Encoder(loop, **kwargs)
    inst.run_app()


def run_generator(loop: asyncio.AbstractEventLoop, args: argparse.Namespace):
    env_map = {
        "out_path": "GEN_DST",
        "pg_uri": "GEN_PG_URI",
        "time_format": "TIME_FORMAT",
    }
    kwargs = merge_env_args(env_map, args)
    re_path = re.compile(r'\[(?P<type>\w+)\](?P<path>.+)')
    src_paths: Dict[str, str] = {}
    if args.src:
        check_paths = args.src
    else:
        check_paths = [v for k, v in os.environ.items() if k.startswith('GEN_SRC')]
    for v in check_paths:
        if m := re_path.match(v):
            if not os.path.exists(m.group('path')):
                print(f'Source directory cannot be found: {v}')
                exit(0)
            src_paths[m.group('type')] = m.group('path')
    kwargs['src_paths'] = src_paths
    if not kwargs.get('pg_uri'):
        raise RuntimeError('PostgreSQL URI is required')
    elif not kwargs.get('src_paths'):
        raise RuntimeError('Source path(s) required')
    elif not kwargs.get('out_path'):
        raise RuntimeError('Output path required')
    inst = Generator(loop, **kwargs)

    async def _run():
        await inst.init_task
        await inst.replace_all()
        try:
            await inst.check_new_files()
        except KeyboardInterrupt:
            inst.logger.info("Keyboard interrupt, exit.")
        except Exception as error:
            traceback.print_exception(type(error), error, error.__traceback__)
        await inst.close()

    loop.run_until_complete(_run())


def run_notifier(loop: asyncio.AbstractEventLoop, args: argparse.Namespace):
    env_map = {
        "mention_id": "NOT_MENTION_ID",
        "webhook_url": "WEBHOOK_URL",
    }
    kwargs = merge_env_args(env_map, args)
    content = ' '.join(args.content)
    inst = Notifier(loop, **kwargs)

    async def _run():
        await inst.init_task
        await inst.send(content=content)

    loop.run_until_complete(_run())


def run_recorder(loop: asyncio.AbstractEventLoop, args: argparse.Namespace):
    env_map = {
        "dry_run": None,
        "busy_file": "BUSY_FILE",
        "enc_path": "ENC_PATH",
        "no_notifications": None,
        "out_path": "REC_OUT",
        "time_format": "TIME_FORMAT",
        "timeout": "REC_TIMEOUT",
        "twitch_id": "REC_TWITCH_ID",
        "user": "REC_USER",
        "webhook_url": "WEBHOOK_URL",
    }
    kwargs = merge_env_args(env_map, args)
    inst = Recorder(loop=loop, **kwargs)

    async def _run():
        await inst.init_task
        while True:
            try:
                await inst.check_if_live()
            except KeyboardInterrupt:
                inst.logger.info("Keyboard interrupt, exit.")
                break
            except Exception as error:
                traceback.print_exception(type(error), error, error.__traceback__)
                pass
        await inst.close()

    loop.run_until_complete(_run())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Launcher for twitch recorder stuff')
    # Global options
    grp_global = parser.add_argument_group(title='Global options')
    grp_global.add_argument('-n', '--dry_run', action='store_true', default=False, help='Do not change any files')
    grp_global.add_argument('--no_notifications', action='store_true', default=False, help='Disable Discord notifications')
    grp_global.add_argument('--time_format', type=str, default='%y%m%d-%H%M', help='Time format string for videos, must not contain _')
    grp_global.add_argument('--webhook_url', type=str, help='Discord webhook URL')
    grp_global.add_argument('--busy_file', type=str, help='Touch a file if we are busy and should not be stopped')

    # Cleaner options
    grp_cleaner = parser.add_argument_group(title='Cleaner options', description='Applies to standalone cleaner and encoder')
    grp_cleaner.add_argument('-d', '--clean_days', type=int, required=False, help='How many days until a file gets deleted')
    grp_cleaner.add_argument('-w', '--warn_at', type=int, action='append', help='Hours before deletion time to send notifications')

    subparsers = parser.add_subparsers(title='Commands', required=True)

    parser_cln = subparsers.add_parser('cleaner', help='Start cleaner, usually started by encoder')
    parser_cln.add_argument('-p', '--path', type=str, required=True, help='Path to check')
    parser_cln.set_defaults(func=run_cleaner)

    parser_enc = subparsers.add_parser('encoder', help='Start encoder REST API server')
    parser_enc.add_argument('-i', '--src_path', type=str, help='Source file location')
    parser_enc.add_argument('-o', '--out_path', type=str, help='Processed file location')
    parser_enc.add_argument('--copy_pattern', type=str, required=False, help='Regex pattern for copying files')
    parser_enc.add_argument('--copy_pattern_opt', type=str, action='append', help='Python Regex compile options, case sensitive')
    parser_enc.add_argument('--enable_cleaner', action='store_true', help='Delete old files not included in processing pattern')
    parser_enc.add_argument('--hevc_pattern', type=str, required=False, help='Regex pattern for HEVC transcoding, takes precedence')
    parser_enc.add_argument('--hevc_pattern_opt', type=str, action='append', help='Python Regex compile options, case sensitive')
    parser_enc.add_argument('--listen_address', type=str, required=False, help='Absolute path (socket) or IP:PORT')
    parser_enc.add_argument('--print_every', action='store_true', help='Print progress after encoding X seconds')
    parser_enc.set_defaults(func=run_encoder)

    parser_gen = subparsers.add_parser('generator', help='Start UUID generator')
    parser_gen.add_argument('-i', '--src', type=str, action='append', help='Source path, formatted as [<type>]<path>')
    parser_gen.add_argument('-o', '--out_path', type=str, required=False, help='Path where symlinks are placed')
    parser_gen.add_argument('-p', '--pg_uri', type=str, required=False, help='PostgreSQL URI')
    parser_gen.set_defaults(func=run_generator)

    parser_not = subparsers.add_parser('notifier', help='Start Discord notifier, primarily for testing')
    parser_not.add_argument('-c', '--content', nargs='+', required=True, help='Content to send')
    parser_not.add_argument('-m', '--mention-id', type=str, help='User ID to mention')
    parser_not.set_defaults(func=run_notifier)

    parser_rec = subparsers.add_parser('recorder', help='Start Twitch recorder')
    parser_rec.add_argument('-u', '--user', type=str, required=False, help='User to record')
    parser_rec.add_argument('-tid', '--twitch_id', type=str, required=False, help='Twitch API Client-ID')
    parser_rec.add_argument('-t', '--timeout', type=int, required=False, help='Time between live checks')
    parser_rec.add_argument('-o', '--out_path', type=str, required=False, help='Output directory of raw recordings')
    parser_rec.add_argument('-e', '--enc_path', type=str, required=False, help='Path/URL to encoder API')
    parser_rec.set_defaults(func=run_recorder)

    return parser.parse_args()


def setup_logger() -> logging.Logger:
    log_fmt = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(log_fmt)
    # File Handler
    if not os.path.exists('log'):
        os.mkdir('log')
    fh = RotatingFileHandler(
        filename=f'log/twitch.log',
        maxBytes=int(1e6), backupCount=3,
        encoding='utf-8', mode='a'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(log_fmt)
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    root_logger.addHandler(fh)
    root_logger.addHandler(ch)
    logger = logging.getLogger("Twitch")
    logger.setLevel(logging.DEBUG)
    return logger


def main():
    logger = setup_logger()
    if not os.path.exists('data'):
        os.mkdir('data')
    args = parse_args()
    loop = asyncio.get_event_loop()
    try:
        args.func(loop, args)
    finally:
        if args.busy_file and os.path.exists(args.busy_file):
            os.unlink(args.busy_file)


if __name__ == '__main__':
    main()
