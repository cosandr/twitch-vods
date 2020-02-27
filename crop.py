import logging
import os
import time
from logging.handlers import RotatingFileHandler
from typing import List

import cv2
import numpy as np
from skimage.metrics import structural_similarity


from utils import run_ffmpeg


class Cropper:

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
        ch.setLevel(logging.INFO)
        ch.setFormatter(log_fmt)
        self.logger.addHandler(ch)
        fh = RotatingFileHandler(
            filename=f'log/cropper.log',
            maxBytes=int(1e6), backupCount=3,
            encoding='utf-8', mode='a'
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(log_fmt)
        self.logger.addHandler(fh)
        self.logger.info("Video cropper started with PID %d", os.getpid())
        # Load reference frame data
        ref_file = 'crop-reference.png'
        if not os.path.exists(ref_file):
            raise FileNotFoundError('Reference file not found')
        self.ref = cv2.imread(ref_file, cv2.IMREAD_GRAYSCALE)
        self.ref_regions = self._crop_to_regions(self.ref)

    async def run_crop(self, file: str, out_file: str):
        """Find intro start and crop out start idle period"""
        # ffmpeg -ss 00:01:00 -i input.mp4 -c copy output.mp4
        intro_seconds = self.find_intro(file=file)
        args = ['-ss', str(intro_seconds), '-i', file, '-c', 'copy', out_file]
        try:
            await run_ffmpeg(logger=self.logger, args=args)
        except Exception as e:
            self.logger.error('Failed to trim %s from %d seconds: %s', file, intro_seconds, str(e))

    def is_start_wait(self, file: str, check_time: int) -> bool:
        """Return True is image is determined to be idle period before intro"""
        r_img = self._crop_to_regions(self.extract_frame(file, seconds=check_time))
        errors = []
        for i in range(len(self.ref_regions)):
            err = structural_similarity(self.ref_regions[i], r_img[i])
            errors.append(err)
        self.logger.debug('Similarity [avg %s]: %s', np.average(errors), errors)
        if np.average(errors) < 0.6:
            return False
        return True

    def _crop_to_regions(self, img: np.ndarray) -> List[np.ndarray]:
        """Returns regions defined by check_areas"""
        ret = []
        for region in self.check_areas:
            from_x = region['start'][0]
            from_y = region['start'][1]
            to_x = from_x + region['size'][0]
            to_y = from_y + region['size'][1]
            # Don't overflow
            to_x = to_x if to_x < img.shape[1] else img.shape[1] - 1
            to_y = to_y if to_y < img.shape[0] else img.shape[0] - 1
            ret.append(img[from_y:to_y, from_x:to_x])
        return ret

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
        self.logger.info('Found intro at %d in %.2fms [%d iterations]', curr_t, (time.perf_counter()-start)*1000, num_iter)
        return curr_t

    def extract_frame(self, video_file: str, frame: int = 0, seconds: int = 0) -> np.ndarray:
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
        return greyscale
