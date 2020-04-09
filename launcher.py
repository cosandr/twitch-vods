import argparse
import asyncio
import os
import traceback

from modules import Encoder, Recorder, Generator

parser = argparse.ArgumentParser(description='Launcher for twitch recorder stuff')
parser.add_argument('cmd', type=str, nargs=1,
                    help="Command to run", choices=['encoder', 'recorder', 'uuid'])

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    for folder in ('data', 'log'):
        if not os.path.exists(folder):
            os.mkdir(folder)
    args = parser.parse_args()
    cmd = args.cmd[0]
    env = os.environ
    if cmd == 'encoder':
        encoder = Encoder(loop=loop)
        while True:
            try:
                loop.run_until_complete(encoder.job_wait())
            except KeyboardInterrupt:
                encoder.logger.info("Keyboard interrupt, exit.")
                break
            except Exception as error:
                traceback.print_exception(type(error), error, error.__traceback__)
                pass
        # Stop server
        loop.run_until_complete(encoder.close())
    elif cmd == 'recorder':
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
    elif cmd == 'uuid':
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
        print(f'Unrecognized command {cmd}')
