import requests
import json
import pickle
import argparse
import datetime
import os
import socket
import subprocess
import streamlink
import re
import sys
import threading
import queue
import time

# Offline
# {'data': [], 'pagination': {'cursor': 'eyJiIjpudWxsLCJhIjp7Ik9mZnNldCI6MH19'}}
# Live
# {'data': [{'id': '30695615760', 'user_id': '31239503', 'game_id': '32399', 'community_ids': ['4bef9cc9-c81d-4384-a62e-b16cf5e7a45e'], 'type': 'live', 'title': 'RERUN: EnVyUs vs. Liquid [Inferno] Map 1 - NA Matchday 4 - ESL Pro League Season 8', 'viewer_count': 247, 'started_at': '2018-10-08T21:52:42Z', 'language': 'en', 'thumbnail_url': 'https://static-cdn.jtvnw.net/previews-ttv/live_user_esl_csgo-{width}x{height}.jpg'}], 'pagination': {'cursor': 'eyJiIjpudWxsLCJhIjp7Ik9mZnNldCI6MX19'}}

# subprocess.call(["streamlink","https://twitch.tv/"+user,quality,"-o", rawpath])
# subprocess.call(['ffmpeg', '-i', rawpath, '-err_detect', 'ignore_err', '-f', 'mp4', '-acodec', 'aac', '-c', 'copy', procpath])
# ffmpeg -i /volume2/docker/Twitch/raw/RichardLewisReports_26-02-19_2347_Return_Of_By_The_Numbers_68.flv -err_detect ignore_err -f mp4 -acodec aac -c copy -y -v info -hide_banner -bsf:a aac_adtstoasc "/volume2/video/Twitch VODs/RichardLewisReports/RichardLewisReports_26-02-19_2347_Return Of By The Numbers #68.mp4"


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
    threadQueue = None
    waitThread = None
    stopEvent = None

    def __init__(self, client_id, user, timeout=120):
        self.client_id = client_id
        self.user = user
        self.TIMEOUT = timeout
        self.threadQueue = queue.Queue()
        self.stopEvent = threading.Event()
        self.waitThread = threading.Thread(target=self.wait)
        self.waitThread.start()

    def wait(self):
        while True:
            item = self.threadQueue.get()
            if item is None:
                self.stopEvent.set()
                break
            if item[0] == "wait":
                # print("Wait thread got wait command.")
                time.sleep(item[1])
                self.threadQueue.task_done()

    def check_if_live(self):
        while not self.stopEvent.is_set():
            # print("Live check waiting.")
            self.threadQueue.join()
            # print("Live check starting.")
            headers = {'Client-ID': self.client_id}
            url = "https://api.twitch.tv/helix/streams"
            params = {'user_login': self.user}
            r = requests.get(url, params=params, headers=headers)
            # Invalid response, just retry.
            if r.status_code != 200:
                print(F"Response error, retrying in {self.TIMEOUT}s.")
                self.threadQueue.put(("wait", self.TIMEOUT))

            # Valid response
            else:
                r_dict = r.json()
                # print(r_dict)
                # Check if live
                if len(r_dict['data']) != 0:
                    if (r_dict['data'][0]['type'] == 'live'):
                        self.title = r_dict['data'][0]['title'].replace("/", "")
                        self.start_time = datetime.datetime.now()
                        self.record()
                else:
                    print(F"{self.user} is offline, retrying in {self.TIMEOUT}s.")
                    self.threadQueue.put(("wait", self.TIMEOUT))

    def record(self):
        no_space_title = re.sub(r'[^a-zA-Z0-9]+', '_', self.title)
        start_time_str = self.start_time.strftime("%y%m%d-%H%M")
        rec_name = F"{start_time_str}_{self.user}_{no_space_title}"
        save_dir = F"/mnt/raw/{rec_name}.flv"
        stream_url = F"twitch.tv/{self.user}"
        print(F"Saving raw stream to {save_dir}")
        try:
            subprocess.run(["streamlink", stream_url, '--default-stream', 'best',
                            "-o", save_dir, "-l", "info"], check=True)
        except Exception:
            if (os.path.exists(save_dir)):
                print("### Stream record error, raw file exists, CONVERT. ###")
                self.threadQueue.put(("wait", self.TIMEOUT))
            else:
                print("### Stream record error, RETRY in 30s. ###")
                self.threadQueue.put(("wait", 30))
        finally:
            self.threadQueue.put(("wait", self.TIMEOUT))
            if (not os.path.exists(save_dir)):
                print("### Stream record complete, NO RAW FILE. ###")
                self.threadQueue.put(("wait", self.TIMEOUT))
            else:
                resp = self.socket_cmd({'cmd': 'single', 'args': {'title': self.title, 'start_time': self.start_time,
                                        'user': self.user, 'source': F"{rec_name}.flv", 'codec': 'copy'}
                                        })

    @staticmethod
    def socket_cmd(cmd):
        if os.path.exists("/mnt/transcode/transcode.sock"):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect("/mnt/transcode/transcode.sock")
            sock.sendall(pickle.dumps(cmd))
            data = sock.recv(1024)
            resp = pickle.loads(data)
            sock.close()
            return resp
        else:
            print("Transcode socket file not found.")

    def stop(self):
        self.threadQueue.put(None)
        self.waitThread.join()


if __name__ == '__main__':
    with open('auth.json', 'r') as fr:
        client_id = json.load(fr)['ID']
    args = parse_args()
    c = Check(client_id, args['user'], timeout=args['timeout'])
    try:
        c.check_if_live()
    except KeyboardInterrupt:
        c.stop()
