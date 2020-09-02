import logging
import os
import time
from typing import List, Optional

import cv2
import numpy as np
from skimage.metrics import structural_similarity

from utils import run_ffmpeg, setup_logger
from .config import Config
from .utils import crop_to_regions


class IntroTrimmer:
    def __init__(self, cfg_path: str, **kwargs):
        self.debug: int = kwargs.get('debug', 0)
        log_parent: str = kwargs.get('log_parent', '')
        # Seconds of accuracy to use when searching for intro
        self.tol: int = kwargs.pop('tol', 10)
        # Seconds to skip forwards the first time
        self.initial_gap: int = kwargs.pop('initial_gap', 300)
        # --- Logger ---
        logger_name = self.__class__.__name__
        if log_parent:
            logger_name = f'{log_parent}.{logger_name}'
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        if not log_parent:
            setup_logger(self.logger, 'cropper')
        # --- Logger ---
        self.logger.info("Video cropper started with PID %d", os.getpid())
        # Load config
        self.cfg: List[Config] = Config.from_json(cfg_path)

    def get_cfg(self, name: str) -> Optional[Config]:
        for c in self.cfg:
            if c.re.search(name):
                return c
        return None

    async def run_crop(self, file: str, out_file: str):
        """Find intro start and crop out start idle period"""
        # ffmpeg -ss 00:01:00 -i input.mp4 -c copy output.mp4
        intro_seconds = self.find_intro(file=file)
        if intro_seconds is None:
            raise RuntimeError(f'No trim definition for {file}')
        args = ['-ss', str(intro_seconds), '-i', file, '-c', 'copy', out_file]
        try:
            await run_ffmpeg(logger=self.logger, args=args)
        except Exception as e:
            self.logger.error('Failed to trim %s from %d seconds: %s', file, intro_seconds, str(e))

    def is_start_wait(self, file: str, check_time: int, cfg: Config) -> bool:
        """Return True is image is determined to be idle period before intro"""
        r_img = crop_to_regions(self.extract_frame(file, seconds=check_time), cfg.check_areas)
        errors = []
        for i in range(len(cfg.regions)):
            err = structural_similarity(cfg.regions[i], r_img[i])
            errors.append(err)
        self.logger.debug('Similarity [avg %s]: %s', np.average(errors), errors)
        if np.average(errors) < 0.6:
            return False
        return True

    def find_intro(self, file: str, test: int = 0) -> Optional[int]:
        """
        Returns time in seconds where the intro starts
        Returns None if we don't have a definition for the file
        """
        cfg: Optional[Config] = self.get_cfg(file)
        if not cfg and not test:
            return None
        curr_t = 0
        gap = self.initial_gap
        # Assume start is intro
        prev_is_intro = True
        if test:
            def _is_start_wait(_f, t, _cfg):
                """Return True when t is smaller than test seconds"""
                return t < test
            is_start_wait = _is_start_wait
        else:
            is_start_wait = self.is_start_wait
        max_iter = 20
        num_iter = 0
        start = time.perf_counter()
        while True:
            curr_t += gap
            curr_is_intro = is_start_wait(file, curr_t, cfg)
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
        cap.release()
        return greyscale
