#!/usr/bin/env python3

import argparse
import asyncio
import os
import traceback

from modules import Encoder, Recorder, Generator, Notifier, Cleaner


LOOP = asyncio.get_event_loop()


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
            kwargs[k] = v
    return kwargs


def run_recorder(args: argparse.Namespace):
    env_map = {
        "user": "REC_USER",
        "twitch_id": "REC_TWITCH_ID",
        "timeout": "REC_TIMEOUT",
        "out_path": "REC_OUT",
        "enc_path": "ENC_LISTEN_ADDRESS",
    }
    kwargs = merge_env_args(env_map, args)
    r = Recorder(loop=LOOP, no_notifications=args.no_notifications, **kwargs)
    while True:
        try:
            LOOP.run_until_complete(r.check_if_live())
        except KeyboardInterrupt:
            r.logger.info("Keyboard interrupt, exit.")
            break
        except Exception as error:
            traceback.print_exception(type(error), error, error.__traceback__)
            pass
    LOOP.run_until_complete(r.close())


parser = argparse.ArgumentParser(description='Launcher for twitch recorder stuff')
# Global options
parser.add_argument('--no_notifications', action='store_true', default=False,
                    help='Disable Discord notifications')
parser.add_argument('-n', '--dry_run', action='store_true', default=False,
                    help='Do not change any files')

subparsers = parser.add_subparsers(title='Commands')

parser_rec = subparsers.add_parser('recorder')
parser_rec.add_argument('-u', '--user', type=str, required=False, help='User to record')
parser_rec.add_argument('-tid', '--twitch_id', type=str, required=False, help='Twitch API Client-ID')
parser_rec.add_argument('-t', '--timeout', type=int, required=False, help='Time between live checks')
parser_rec.add_argument('-o', '--out_path', type=str, required=False, help='Output directory of raw recordings')
parser_rec.add_argument('-e', '--enc_path', type=str, required=False, help='Path/URL to encoder API')
parser_rec.set_defaults(func=run_recorder)


if __name__ == '__main__':
    for folder in ('data', 'log'):
        if not os.path.exists(folder):
            os.mkdir(folder)
    _args = parser.parse_args()
    _args.func(_args)
    exit(0)
    # if args.cmd == 'encoder':
    #     encoder = Encoder(loop=loop, convert_non_btn=args.convert_non_btn, always_copy=args.always_copy,
    #                       print_every=args.print_every, enable_notifications=not args.no_notifications,
    #                       dry_run=args.dry_run, manual_run=args.manual)
    #     manual_ran = False
    #     while True:
    #         if manual_ran:
    #             break
    #         try:
    #             if args.manual and args.job_num is not None:
    #                 manual_ran = True
    #                 encoder.mark_job(args.job_num)
    #             loop.run_until_complete(encoder.job_wait())
    #         except KeyboardInterrupt:
    #             encoder.logger.info("Keyboard interrupt, exit.")
    #             break
    #         except Exception as error:
    #             traceback.print_exception(type(error), error, error.__traceback__)
    #             pass
    #     # Stop server
    #     loop.run_until_complete(encoder.close())
    # elif args.cmd == 'recorder':
    #     rec = Recorder(loop, enable_notifications=not args.no_notifications)
    #     while True:
    #         try:
    #             loop.run_until_complete(rec.check_if_live())
    #         except KeyboardInterrupt:
    #             rec.logger.info("Keyboard interrupt, exit.")
    #             break
    #         except Exception as error:
    #             traceback.print_exception(type(error), error, error.__traceback__)
    #             pass
    #     loop.run_until_complete(rec.close())
    # elif args.cmd == 'uuid':
    #     gen = Generator(loop)
    #     loop.run_until_complete(gen.replace_all())
    #     try:
    #         loop.run_until_complete(gen.check_new_files())
    #     except KeyboardInterrupt:
    #         gen.logger.info("Keyboard interrupt, exit.")
    #     except Exception as error:
    #         traceback.print_exception(type(error), error, error.__traceback__)
    #     loop.run_until_complete(gen.close())
    # elif args.cmd == 'notifier':
    #     if not args.content:
    #         raise Exception('Content is required')
    #     content = ' '.join(args.content)
    #     notifier = Notifier(loop, tcp_host=args.tcp_host, tcp_port=args.tcp_port)
    #     loop.run_until_complete(notifier.send(content=content))
    # elif args.cmd == 'cleaner':
    #     if not args.content:
    #         raise Exception('Content is required (path to check)')
    #     content = ' '.join(args.content)
    #     cleaner = Cleaner(loop, check_path=content, enable_notifications=not args.no_notifications,
    #                       dry_run=args.dry_run)
    #     cleaner.worker_task = loop.create_task(cleaner.worker())
    #     cleaner.en_del.set()
    #     loop.run_until_complete(cleaner.worker_task)
    # else:
    #     print(f'Unrecognized command {args.cmd}')
