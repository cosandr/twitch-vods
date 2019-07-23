# This script checks if a user on twitch is currently streaming and then records the stream via streamlink
import datetime
import re
import subprocess
import sys
import os
import argparse
from requests import exceptions as reqexc
from threading import Timer
from twitch import TwitchClient


def check_user(user):
    global CLIENT_ID
    global VALID_BROADCAST
    """ returns 0: online, 1: offline, 2: not found, 3: error """
    try:
        client = TwitchClient(client_id=CLIENT_ID)
        response = client.users.translate_usernames_to_ids(user)
    except reqexc.HTTPError as ex:
        print("Bad client id: '%s'" %(CLIENT_ID))
        print(ex)
        sys.exit(4)
    stream_info = 0
    if response.__len__() > 0:
        user_id = response[0].id
        stream_info = client.streams.get_stream_by_user(user_id)
        if stream_info is not None:
            status = 0
            # if stream_info.broadcast_platform in VALID_BROADCAST:
            #     status = 0  # user is streaming
            # else:
            #     status = 3  # unexpected error
        else:
            status = 1      # user offline
    else:
        status = 2          # user not found

    return status, stream_info


def loopcheck():
    status, stream_info = check_user(user)
    valid_title = True
    if status == 2:
        print("Username not found. Invalid username?")
        sys.exit(3)
    elif status == 3:
        print("Unexpected error. Maybe try again later")
    elif status == 1:
        t = Timer(time, loopcheck)
        print(user,"is currently offline, checking again in",time,"seconds")
        t.start()
    elif status == 0:
        if FORCE_TITLE != "":
            print(F"Title enforcement enabled. Only record streams with {FORCE_TITLE}.")
            if FORCE_TITLE not in stream_info['channel']['status'].lower():
                print(F"{FORCE_TITLE} not in {stream_info['channel']['status']}.")
                valid_title = False
        if valid_title:
            rec_path = "raw"
            proc_path = "../vods"
            print(user,"is online. Stop.")
            filename = user+"_"+datetime.datetime.now().strftime("%Y-%m-%d_%H.%M")+"_"+re.sub(r"[^a-zA-Z0-9]+", '_', stream_info['channel']['status'])
            if not os.path.exists(rec_path):
                os.makedirs(rec_path)
            rawpath = F"{rec_path}/{filename}.flv"
            procpath = F"{proc_path}/{filename}.mp4"
            print(F"Saving raw stream to {rawpath}")
            subprocess.call(["streamlink","https://twitch.tv/"+user,quality,"-o", rawpath])
            # Convert video
            if not os.path.exists(proc_path):
                os.makedirs(proc_path)
            print(F"Processing to {procpath}")
            subprocess.call(['ffmpeg', '-i', rawpath, '-err_detect', 'ignore_err', '-f', 'mp4', '-acodec', 'aac', '-c', 'copy', procpath])
            # os.remove(rawpath)
            print("Stream is done. Going back to checking...")
            t = Timer(time, loopcheck)
            t.start()
        else:
            print(F"User stream title doesn't match rules. Waiting...")
            t = Timer(time, loopcheck)
            t.start()


def parse_args():
    parser = argparse.ArgumentParser(description='Records Twitch stream.')
    parser.add_argument('-u', '--user', required=True, type=str,
                        help='Twitch username of streamer.')
    parser.add_argument('-t', '--time', required=False, type=int,
                        help='Time to wait between checks.', default=120)
    parser.add_argument('-q', '--quality', required=False, type=str,
                        help='Stream quality to record.', default='best')
    parser.add_argument('--title', required=False, type=str,
                        help='Enforce title of stream.', default='')
    parser.add_argument('-r', '--rerun', action='store_true',
                        help='Allow reruns.')
    args = vars(parser.parse_args())
    return args


if __name__ == "__main__":
    CLIENT_ID_FILE = "client_id.txt"
    try:
        client_file=open(CLIENT_ID_FILE,'r')
    except FileNotFoundError as ex:
        print(ex)
        print("Client id file doesn't exist. Generate ID at https://glass.twitch.tv/console/apps")
        sys.exit(4)
    CLIENT_ID=client_file.read()
    client_file.close()

    args = parse_args()
    VALID_BROADCAST = ['live']
    user = args['user']
    time = args['time']
    quality = args['quality']
    FORCE_TITLE = args['title']
    print(FORCE_TITLE)
    if args['rerun']:
        VALID_BROADCAST.append('rerun')

    if(time<15):
        print("Time shouldn't be lower than 15 seconds")
        time=15

    print("Checking for",user,"every",time,"seconds. Record with",quality,"quality.")
    loopcheck()
