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
        status_str = (
            f'- PID: {os.getpid()}\n'
            f'- Tolerance: {self.tol}\n'
            f'- Initial gap: {self.initial_gap}\n'
        )
        # Load config
        self.cfg: List[Config] = Config.from_json(cfg_path)
        if self.cfg:
            status_str += "- Cropper patterns:\n"
            for c in self.cfg:
                status_str += f'-- {c.re.pattern}\n'
        self.logger.info("\n%s", status_str)

    def get_cfg(self, name: str) -> Optional[Config]:
        for c in self.cfg:
            if c.re.search(name):
                return c
        return None

    async def run_crop(self, file: str, out_file: str, check_name='', cfg=None, use_ms=False) -> Optional[int]:
        """Find intro start and crop out start idle period"""
        # ffmpeg -ss 00:01:00 -i input.mp4 -c copy output.mp4
        intro_seconds = self.find_intro(file=file, cfg=cfg, check_name=check_name, use_ms=use_ms)
        if intro_seconds is None:
            raise RuntimeError(f'No trim definition for {file}')
        args = ['-ss', str(intro_seconds), '-i', file, '-c', 'copy', out_file]
        try:
            await run_ffmpeg(logger=self.logger, args=args)
        except Exception as e:
            self.logger.error('Failed to trim %s from %d seconds: %s', file, intro_seconds, str(e))
        return intro_seconds

    def find_intro(self, file: str, check_name='', cfg=None, use_ms=False, _test=0) -> Optional[int]:
        """
        Returns time in seconds where the intro starts
        Returns None if we don't have a definition for the file
        """
        if cfg is None:
            if check_name:
                cfg: Optional[Config] = self.get_cfg(check_name)
            else:
                cfg: Optional[Config] = self.get_cfg(file)
        if not cfg and not _test:
            return None
        curr_t = 0
        gap = self.initial_gap
        # Assume start is intro
        prev_is_intro = True
        max_iter = 20
        num_iter = 0
        start = time.perf_counter()
        while True:
            curr_t += gap
            if _test:
                curr_is_intro = curr_t < _test
            else:
                curr_is_intro = self.is_start_wait(file, curr_t, cfg, use_ms=use_ms)
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

    def is_start_wait(self, file: str, check_time: int, cfg: Config, use_ms=False) -> bool:
        """Return True is image is determined to be idle period before intro"""
        if use_ms:
            frame = self.extract_frame_ms(file, seconds=check_time)
        else:
            frame = self.extract_frame(file, seconds=check_time)
        r_img = crop_to_regions(frame, cfg.check_areas)
        errors = []
        for i in range(len(cfg.regions)):
            err = structural_similarity(cfg.regions[i], r_img[i])
            errors.append(err)
        self.logger.debug('Similarity [avg %s]: %s', np.average(errors), errors)
        if np.average(errors) < 0.6:
            return False
        return True

    def extract_frame(self, video_file: str, frame: int = 0, seconds: int = 0) -> np.ndarray:
        """Returns 640x360 greyscale frame from video_file"""
        if not os.path.exists(video_file):
            raise FileNotFoundError(f'{video_file} not found')
        start = time.perf_counter()
        cap = cv2.VideoCapture(video_file)
        try:
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if seconds:
                frame = int(seconds * cap.get(cv2.CAP_PROP_FPS))
            if frame > total_frames or frame < 0:
                raise Exception(f'Frames must be between 0 and {total_frames}, got {frame}')
            ok = cap.set(cv2.CAP_PROP_POS_FRAMES, frame)
            if not ok:
                raise Exception(f'CV2 cannot skip to frame {frame:,d}/{total_frames:,d} in {video_file}')
            ok, img = cap.read()
            if not ok:
                raise Exception(f'CV2 cannot read frame {frame:,d}/{total_frames:,d} from {video_file}')
            greyscale = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            self.logger.debug('CV2 read frame %d/%d in %.2fms', frame, total_frames, (time.perf_counter()-start)*1000)
            greyscale = cv2.resize(greyscale, (640, 360), interpolation=cv2.INTER_LINEAR)
            return greyscale
        finally:
            cap.release()

    def extract_frame_ms(self, video_file: str, seconds: int = 0) -> np.ndarray:
        """Appears to be broken"""
        if not os.path.exists(video_file):
            raise FileNotFoundError(f'{video_file} not found')
        ms_target = seconds * 1000
        start = time.perf_counter()
        cap = cv2.VideoCapture(video_file)
        try:
            # Seek to end
            ok = cap.set(cv2.CAP_PROP_POS_AVI_RATIO, 1)
            if not ok:
                raise Exception(f'CV2 [{video_file}] cannot set position to end of file')
            ok, _ = cap.read()
            if not ok:
                raise Exception(f'CV2 [{video_file}] cannot read frame at end of file')
            ms_total = cap.get(cv2.CAP_PROP_POS_MSEC)
            if not ms_total:
                raise Exception(f'CV2 [{video_file}] cannot get milliseconds at end of file')
            if ms_target > ms_total:
                raise RuntimeError(f'Cannot seek to {ms_target:,.1f}ms/{ms_total:,.1f}ms in {video_file}')
            ok = cap.set(cv2.CAP_PROP_POS_MSEC, ms_target)
            if not ok:
                raise Exception(f'CV2 cannot set position to {ms_target:,.1f}ms/{ms_total:,.1f}ms in {video_file}')
            ok, img = cap.read()
            if not ok:
                raise Exception(f'CV2 cannot read frame at {ms_target:,.1f}ms in {video_file}')
            greyscale = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            self.logger.debug('CV2 read frame at %,.1f/%,.1f in %.2fms', ms_target, ms_total, (time.perf_counter()-start)*1000)
            greyscale = cv2.resize(greyscale, (640, 360), interpolation=cv2.INTER_LINEAR)
            return greyscale
        finally:
            cap.release()
