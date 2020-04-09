import argparse
import asyncio
import os
import traceback

from modules import Encoder, Recorder, Generator

parser = argparse.ArgumentParser(description='Launcher for twitch recorder stuff')
parser.add_argument('cmd', type=str,
                    help="Command to run", choices=['encoder', 'recorder', 'uuid'])
parser.add_argument('-m', '--manual', action='store_true', default=False,
                    help='Encoder: Run manually, provide src and user')
parser.add_argument('--job_num', type=int,
                    help='Encoder: Run job with this index')
parser.add_argument('--convert_non_btn', action='store_true', default=False,
                    help='Encoder: Encode non-BTN files with HEVC')
parser.add_argument('--always_copy', action='store_true', default=False,
                    help='Encoder: Always copy raw files, never encode to HEVC or ignore')
parser.add_argument('--print_every', type=int, default=60,
                    help='Encoder: How often to log FFMPEG progress (in seconds)')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    for folder in ('data', 'log'):
        if not os.path.exists(folder):
            os.mkdir(folder)
    args = parser.parse_args()
    if args.cmd == 'encoder':
        encoder = Encoder(loop=loop, convert_non_btn=args.convert_non_btn, always_copy=args.always_copy,
                          print_every=args.print_every)
        manual_ran = False
        while True:
            if manual_ran:
                break
            try:
                if args.manual and args.job_num is not None:
                    manual_ran = True
                    encoder.mark_job(args.job_num)
                loop.run_until_complete(encoder.job_wait())
            except KeyboardInterrupt:
                encoder.logger.info("Keyboard interrupt, exit.")
                break
            except Exception as error:
                traceback.print_exception(type(error), error, error.__traceback__)
                pass
        # Stop server
        loop.run_until_complete(encoder.close())
    elif args.cmd == 'recorder':
        rec = Recorder(loop)
        while True:
            try:
                loop.run_until_complete(rec.check_if_live())
            except KeyboardInterrupt:
                rec.logger.info("Keyboard interrupt, exit.")
                break
            except Exception as error:
                traceback.print_exception(type(error), error, error.__traceback__)
                pass
        loop.run_until_complete(rec.close())
    elif args.cmd == 'uuid':
        gen = Generator(loop)
        loop.run_until_complete(gen.replace_all())
        try:
            loop.run_until_complete(gen.check_new_files())
        except KeyboardInterrupt:
            gen.logger.info("Keyboard interrupt, exit.")
        except Exception as error:
            traceback.print_exception(type(error), error, error.__traceback__)
        loop.run_until_complete(gen.close())
    else:
        print(f'Unrecognized command {args.cmd}')
