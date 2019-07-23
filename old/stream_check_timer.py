import requests
import json
import argparse
import datetime
import os
# import streamlink
# import ffmpeg
import subprocess
import re
import sys
import threading

# Offline
# {'data': [], 'pagination': {'cursor': 'eyJiIjpudWxsLCJhIjp7Ik9mZnNldCI6MH19'}}
# Live
# {'data': [{'id': '30695615760', 'user_id': '31239503', 'game_id': '32399', 'community_ids': ['4bef9cc9-c81d-4384-a62e-b16cf5e7a45e'], 'type': 'live', 'title': 'RERUN: EnVyUs vs. Liquid [Inferno] Map 1 - NA Matchday 4 - ESL Pro League Season 8', 'viewer_count': 247, 'started_at': '2018-10-08T21:52:42Z', 'language': 'en', 'thumbnail_url': 'https://static-cdn.jtvnw.net/previews-ttv/live_user_esl_csgo-{width}x{height}.jpg'}], 'pagination': {'cursor': 'eyJiIjpudWxsLCJhIjp7Ik9mZnNldCI6MX19'}}

# subprocess.call(["streamlink","https://twitch.tv/"+user,quality,"-o", rawpath])
# subprocess.call(['ffmpeg', '-i', rawpath, '-err_detect', 'ignore_err', '-f', 'mp4', '-acodec', 'aac', '-c', 'copy', procpath])
# ffmpeg -i raw/RichardLewisReports_14-11-18_1920_The_Richard_Lewis_Show_132.flv -err_detect ignore_err -f mp4 -acodec aac -c copy -y -v info -hide_banner "vods/RichardLewisReports/The Richard Lewis Show 132_14-11-18_1920_.mp4"

def parse_args():
    parser = argparse.ArgumentParser(description='Twitch live stream recorder.')
    parser.add_argument('-u', '--user', required=True, type=str,
                        help='Twitch username to check.')
    parser.add_argument('-t', '--timeout', required=False, type=int,
                        help='How often to check, keep longer than 60s.',
                        default=120)

    args = vars(parser.parse_args())
    return args


class Check():
    client_id = ""
    user = ""
    TIMEOUT = 120
    title = ''
    start_time = ''

    def __init__(self, client_id, user, timeout=120):
        self.client_id = client_id
        self.user = user
        self.TIMEOUT = timeout

    def wait(self, timeout=10):
        try:
            t = threading.Timer(timeout, self.check_if_live)
            t.start()
        except KeyboardInterrupt:
            t.cancel()
            print('Timer cancelled, exiting.')
            sys.exit(0)

    def check_if_live(self):
        headers = {'Client-ID': self.client_id}
        url = "https://api.twitch.tv/helix/streams"
        params = {'user_login': self.user}
        r = requests.get(url, params=params, headers=headers)
        # Invalid response, just retry.
        if r.status_code != 200:
            print(F"Response error, retrying in {self.TIMEOUT}s.")
            self.wait(self.TIMEOUT)

        # Valid response
        else:
            r_dict = r.json()
            # print(r_dict)
            # Check if live
            if len(r_dict['data']) != 0:
                if (r_dict['data'][0]['type'] == 'live'):
                    self.title = r_dict['data'][0]['title']
                    self.start_time = datetime.datetime.now().strftime("%d-%m-%y_%H%M")
                    self.record()
            else:
                print(F"{self.user} is offline, retrying in {self.TIMEOUT}s.")
                self.wait(self.TIMEOUT)

    def record(self):
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.title)
        rec_name = F"{self.user}_{self.start_time}_{no_space_title}"
        save_dir = F"/mnt/raw/{rec_name}.flv"
        stream_url = F"twitch.tv/{self.user}"
        print(F"Saving raw stream to {save_dir}")
        try:
            try:
                subprocess.run(["streamlink", stream_url, '--default-stream', 'best',
                                "-o", save_dir, "-l", "info"], check=True)
            except Exception:
                print("### Stream record error, retrying in 30s. ###")
                if (os.path.exists(save_dir)):
                    self.convert(save_dir)
                else:
                    self.wait(30)
                return

        except KeyboardInterrupt:
            print("Stream capture cancelled, skip to convert.")

        self.convert(save_dir)

    def convert(self, source):
        conv_name = F"{self.title}_{self.start_time}"
        proc_path = F"/mnt/vods/{self.user}"
        full_path = F"{proc_path}/{conv_name}.mp4"
        if not os.path.exists(proc_path):
            os.makedirs(proc_path)
        print(F"Processing to {full_path}")
        try:
            try:
                subprocess.run(['ffmpeg', '-i', source, '-err_detect', 'ignore_err',
                                '-f', 'mp4', '-acodec', 'aac', '-c', 'copy', '-y',
                                '-v', 'info', '-hide_banner', full_path],
                                check=True)
            except Exception:
                print("### Video copy error, fallback to re-encoding. ###")
                try:
                    subprocess.run(['ffmpeg', '-i', source, '-err_detect', 'ignore_err',
                                    '-f', 'mp4', '-acodec', 'aac', '-vcodec', 'libx264',
                                    '-preset', 'veryfast', '-y', '-v', 'info', '-hide_banner',
                                    full_path], check=True)
                except Exception:
                    print("### FALLBACK CONVERSION FAIL ###")
                    self.wait(10)

        except KeyboardInterrupt:
            print("Conversion cancelled, exiting.")
            sys.exit(0)

        # Delete raw file
        # os.remove(source)
        self.wait(10)


if __name__ == '__main__':
    with open('auth.json', 'r') as fr:
        client_id = json.load(fr)['ID']
    args = parse_args()
    c = Check(client_id, args['user'], timeout=args['timeout'])
    c.check_if_live()
