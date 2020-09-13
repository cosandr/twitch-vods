import os
import platform
import time
import unittest

import cv2
import numpy as np
from skimage.metrics import mean_squared_error, structural_similarity

from modules import IntroTrimmer
from modules.intro_trimmer.config import Config
from modules.intro_trimmer.utils import crop_to_regions

base_video_path = 'media/twitch/RichardLewisReports/'
if platform.system() == 'Windows':
    VIDEO_PATH = f"T:/{base_video_path}"
else:
    VIDEO_PATH = f"/tank/{base_video_path}"
    if platform.node().lower() != 'dresrv':
        VIDEO_PATH = f"/dresrv/{VIDEO_PATH}"

FRAME_PATH = 'test/frames'
CONFIG = Config(
    pattern=r'by\s*the\s*numbers',
    pattern_opt='IGNORECASE',
    image='data/trimmer/btn.png',
    check_areas=[
        {
            "start": [0, 298],
            "size": [640, 52],
        },
        {
            "start": [499, 248],
            "size": [137, 40],
        },
    ],
)

"""
Countdown start 12:33 (753)
Intro at 15:43 (943)
Starts at 16:17 (977)
IN="/tank/media/twitch/RichardLewisReports/200128-2320_Return Of By The Numbers #105.mp4"
ffmpeg -ss 30 -i "$IN"  -s 640x360 -qscale:v 10 -frames:v 1 test_btn105_30s.png
ffmpeg -i "$IN" -ss 30 -s 640x360 -qscale:v 10 -frames:v 1 -c:v png -f image2pipe -
"""


class TestCrop(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cropper = IntroTrimmer(tol=5, initial_gap=300, debug=0, cfg_path='data/trimmer/config.json')

    def test_is_start_wait(self):
        expected_def = {
            '200122-2318_Return Of By The Numbers #102.mp4': {True: np.linspace(0, 1200, num=5, dtype=np.uint16),
                                                              False: np.linspace(1280, 3000, num=5, dtype=np.uint16)},
            '200124-2323_Return Of By The Numbers #104.mp4': {True: np.linspace(0, 720, num=5, dtype=np.uint16),
                                                              False: np.linspace(730, 3000, num=5, dtype=np.uint16)},
            '200128-2320_Return Of By The Numbers #105.mp4': {True: np.linspace(0, 940, num=5, dtype=np.uint16),
                                                              False: np.linspace(950, 3000, num=5, dtype=np.uint16)},
        }
        for file, dct in expected_def.items():
            for expected, values in dct.items():
                for val in values:
                    actual = self.cropper.is_start_wait(VIDEO_PATH + file, val, CONFIG)
                    self.assertEqual(expected, actual, f'{file} [{val}]')

    def test_compare(self):
        for f in os.listdir(FRAME_PATH):
            if not f.endswith('.png'):
                continue
            fp = os.path.join(FRAME_PATH, f)
            img = cv2.imread(fp, cv2.IMREAD_GRAYSCALE)
            start = time.perf_counter()
            mse_err = mean_squared_error(CONFIG.image, img)
            mse_time = (time.perf_counter() - start)*1000
            start = time.perf_counter()
            ssim_err = structural_similarity(CONFIG.image, img)
            ssim_time = (time.perf_counter() - start)*1000
            print('%s: MSE [%.2fms] %.2f, SSIM [%.2fms] %.2f' % (fp, mse_time, mse_err, ssim_time, ssim_err))

    def test_compare_regions(self):
        for f in os.listdir(FRAME_PATH):
            if not f.endswith('.png'):
                continue
            fp = os.path.join(FRAME_PATH, f)
            img = cv2.imread(fp, cv2.IMREAD_GRAYSCALE)
            r_img = crop_to_regions(img, CONFIG.check_areas)
            for i in range(len(CONFIG.regions)):
                start = time.perf_counter()
                mse_err = mean_squared_error(CONFIG.regions[i], r_img[i])
                mse_time = (time.perf_counter() - start)*1000
                start = time.perf_counter()
                ssim_err = structural_similarity(CONFIG.regions[i], r_img[i])
                ssim_time = (time.perf_counter() - start)*1000
                print('%s region %d: MSE [%.2fms] %.2f, SSIM [%.2fms] %.2f' % (fp, i, mse_time, mse_err, ssim_time, ssim_err))
                i += 1

    def test_find_intro(self):
        expected = {
            '200122-2318_Return Of By The Numbers #102.mp4': 1277,
            '200124-2323_Return Of By The Numbers #104.mp4': 722,
            '200128-2320_Return Of By The Numbers #105.mp4': 944,
        }
        for file, val in expected.items():
            actual = self.cropper.find_intro(VIDEO_PATH+file)
            self.assertEqual(abs(val - actual) <= self.cropper.tol, True, f'Expected: {val}s, got {actual}s, diff {abs(actual-val)}s')

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
            actual = self.cropper.find_intro('', _test=val)
            self.assertEqual(abs(val - actual) <= self.cropper.tol, True, f'Expected: {val}s, got {actual}s, diff {abs(actual-val)}s')

    @unittest.skip('Run to setup data when needed')
    def test_extract(self):
        extract_from = {
            '200122-2318_Return Of By The Numbers #102.mp4': {'actual': [1277], 'before': range(0, 1270, 300), 'after': range(1280, 3000, 300)},
            '200124-2323_Return Of By The Numbers #104.mp4': {'actual': [722], 'before': range(0, 710, 300), 'after': range(730, 2000, 300)},
            '200128-2320_Return Of By The Numbers #105.mp4': {'actual': [944], 'before': range(0, 940, 300), 'after': range(950, 3000, 300)},
        }
        for file, time_def in extract_from.items():
            for name, times in time_def.items():
                for t in times:
                    arr = self.cropper.extract_frame(VIDEO_PATH+file, seconds=t)
                    fp = os.path.join(FRAME_PATH, f'{file.split("_", 1)[0]}_{name}_{t}.png')
                    cv2.imwrite(fp, arr)


if __name__ == '__main__':
    unittest.main()
