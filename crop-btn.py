import asyncio
import logging
import os
from io import BytesIO
from logging.handlers import RotatingFileHandler
import time
from typing import Optional
from PIL import Image
import numpy as np
import cv2


"""
Countdown start 12:33 (753)
Intro at 15:43 (943)
Starts at 16:17 (977)
IN="/tank/media/twitch/RichardLewisReports/200128-2320_Return Of By The Numbers #105.mp4"
ffmpeg -i "$IN" -ss 30 -s 640x360 -qscale:v 10 -frames:v 1 test_btn105_30s.png
ffmpeg -i "$IN" -ss 30 -s 640x360 -qscale:v 10 -frames:v 1 -c:v png -f image2pipe -
"""

VIDEO_PATH = "Z:/media/twitch/RichardLewisReports/"

class Cropper:

    common_args = [
        '-s', '640x360', '-c:v', 'png', '-frames:v', '1', '-f', 'image2pipe',
        # '-v', 'warning', '-y', '-progress', '-', '-nostats', '-hide_banner',
    ]

    check_areas = [
        # Big banner at the bottom
        {
            'start': [0, 298],
            'size': [640, 52],
        },
        # Your hosts and timer
        {
            'start': [499, 248],
            'size': [137, 40],
        },
    ]

    def __init__(self, tol: int = 10, initial_gap: int = 300, debug: int = 0):
        self.debug = debug
        # Seconds of accuracy to use when searching for intro
        self.tol = tol
        # Seconds to skip forwards the first time
        self.initial_gap = initial_gap
        # --- Logger ---
        log_fmt = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
        self.logger = logging.getLogger('cropper')
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists('log'):
            os.mkdir('log')
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(log_fmt)
        self.logger.addHandler(ch)
        fh = RotatingFileHandler(
            filename=f'log/cropper.log',
            maxBytes=1e6, backupCount=3,
            encoding='utf-8', mode='a'
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(log_fmt)
        self.logger.addHandler(fh)
        self.logger.info("Video cropper started with PID %d", os.getpid())
        # Load reference frame data
        self.ref = self._calc_block_avg(cv2.imread('test_cv2.png', cv2.IMREAD_GRAYSCALE))

    def _calc_block_avg(self, arr: np.ndarray) -> list:
        results = []
        # Checking 10x10 pixels at a time
        block_size = 10
        for area in self.check_areas:
            start_x, start_y = area['start'][0], area['start'][1]
            # Ensure we don't go over bounds
            max_x = area['size'][0]+start_x if area['size'][0]+start_x < arr.shape[1]-1 else arr.shape[1]-1
            max_y = area['size'][1]+start_y if area['size'][1]+start_y < arr.shape[0]-1 else arr.shape[0]-1
            if self.debug > 1:
                self.logger.debug('Calculating area for %d, %d image starting at %d, %d to max %d, %d', arr.shape[1], arr.shape[0], start_x, start_y, max_x, max_y)
            start = time.perf_counter()
            # Block average temp variable
            tmp = []
            x, y = start_x, start_y
            while True:
                if x > max_x and y > max_y:
                    break
                elif x > max_x:
                    x = start_x
                elif y > max_y:
                    y = start_y
                tmp.append(arr[y][x])
                x += 1
                y += 1
                # if x % block_size == 0:
                #     y += 1
            results.append(np.average(tmp))
            if self.debug > 1:
                self.logger.debug('Area avg %.2f calculated in %.2fms', results[-1], (time.perf_counter()-start)*1000)
        return results

    def is_start_wait(self, file: str, check_time: int) -> bool:
        """Return True is image is determined to be idle period before intro"""
        # img = asyncio.run(self.extract_frame(TEST_FILE, check_time))
        area_avg = self._calc_block_avg(self.cv2_extract_frame(file, seconds=check_time))
        errors = []
        for i in range(len(area_avg)):
            if i > len(self.ref):
                continue
            errors.append(abs(area_avg[i]-self.ref[i]))
        self.logger.debug('Errors [avg %s]: %s', np.average(errors), errors)
        if np.average(errors) > 30:
            return False
        return True

    def find_intro(self, file: str, test: int = 0) -> int:
        """Returns time in seconds where the intro starts"""
        curr_t = 0
        gap = self.initial_gap
        # Assume start is intro
        prev_is_intro = True
        if test:
            # Return True when t is smaller than test seconds
            is_start_wait = lambda f, t: True if t < test else False
        else:
            is_start_wait = self.is_start_wait
        max_iter = 20
        num_iter = 0
        start = time.perf_counter()
        while True:
            curr_t += gap
            curr_is_intro = is_start_wait(file, curr_t)
            if self.debug > 0:
                self.logger.debug('%d: was %s, is %s, gap %d', curr_t, prev_is_intro, curr_is_intro, gap)
            if abs(gap) <= self.tol:
                break
            elif curr_is_intro and prev_is_intro:
                gap = abs(gap)
            elif curr_is_intro and not prev_is_intro:
                gap = int(abs(gap)/2)
            elif not curr_is_intro:
                gap = -int(abs(gap)/2)
            prev_is_intro = curr_is_intro
            num_iter += 1
            if num_iter >= max_iter:
                break
        self.logger.debug('Found intro in %.2fms [%d iterations]', (time.perf_counter()-start)*1000, num_iter)
        return curr_t

    def test_find_intro(self):
        expected = {
            '200122-2318_Return Of By The Numbers #102.mp4': 1277,
            '200124-2323_Return Of By The Numbers #104.mp4': 722,
            '200128-2320_Return Of By The Numbers #105.mp4': 944,
        }
        for file, val in expected.items():
            actual = self.find_intro(VIDEO_PATH+file)
            print(f'Expected: {val}s, got {actual}s, diff {abs(actual-val)}s')

    def test_find_intro_artificial(self):
        # Check 300 seconds
        # Intro, check 600 seconds <- 300 + 300, gap 300
        # Intro, check 900 seconds <- 600 + 300, gap 300
        # Not intro, check 750 <- 900-(900-600)/2, pivot was 900, gap 150
        # Not intro, check 675 <- 750-(900-750)/2, pivot was 750, gap 75
        # Intro, check 712.5 <- 675+(750-675)/2, pivot was 675, gap 37.5
        # Intro, but within tolerance
        expected = [20, 40, 60, 67, 123, 333, 444, 777, 722, 944, 1277]
        for val in expected:
            actual = self.find_intro('', test=val)
            print(f'Expected: {val}s, got {actual}s, diff {abs(actual-val)}s')

    async def extract_frame(self, video_file: str, seconds: int) -> Optional[Image.Image]:
        """Returns an Image from the video file at seconds"""
        args = self.common_args.copy()
        # Insert input
        args.insert(0, '-i')
        args.insert(1, video_file)
        args.insert(2, '-ss')
        args.insert(3, str(seconds))
        # Append output file
        args.append('-')
        self.logger.debug('CMD: ffmpeg %s', ' '.join(args))
        start = time.perf_counter()
        p = await asyncio.create_subprocess_exec('ffmpeg', *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await p.communicate()
        if p.returncode != 0:
            print(f'ffmpeg failed: {stderr}')
            return
        buf = BytesIO(stdout)
        img = Image.open(buf)
        self.logger.debug('Opened %s image in %.2fms', img.size, (time.perf_counter()-start)*1000)
        return img

    def cv2_extract_frame(self, video_file: str, frame: int = 0, seconds: int = 0) -> np.ndarray:
        if not os.path.exists(video_file):
            raise FileNotFoundError(f'{video_file} not found')
        start = time.perf_counter()
        cap = cv2.VideoCapture(video_file)
        total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        if seconds:
            frame = int(seconds * cap.get(cv2.CAP_PROP_FPS))
        if frame > total_frames or frame < 0:
            raise Exception(f'Frames must be between 0 and {total_frames}, got {frame}')
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame)
        res, img = cap.read()
        greyscale = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        self.logger.debug('CV2 read frame %d/%d in %.2fms', frame, total_frames, (time.perf_counter()-start)*1000)
        greyscale = cv2.resize(greyscale, (640, 360), interpolation=cv2.INTER_LINEAR)
        # cv2.imwrite('test_cv2.png', greyscale)
        return greyscale


if __name__ == "__main__":
    c = Cropper(tol=5)
    # asyncio.run(c.extract_frame(test_file, 30))
    # c.cv2_extract_frame(TEST_FILE, 100)
    c.test_find_intro_artificial()
