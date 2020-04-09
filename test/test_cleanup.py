import platform
import random
import unittest
from datetime import datetime, timedelta

import config as cfg
import utils

base_video_path = 'media/twitch/RichardLewisReports/'
if platform.system() == 'Windows':
    VIDEO_PATH = f"Z:/{base_video_path}"
else:
    VIDEO_PATH = f"/tank/{base_video_path}"
    if platform.node().lower() != 'dresrv':
        VIDEO_PATH = f"/mnt/sshfs/{VIDEO_PATH}"


class TestCrop(unittest.TestCase):
    @staticmethod
    def generate_names(num_files=5, ext='flv'):
        """Generate file names according to format in config"""
        date = datetime.now()
        names = []
        for i in range(num_files):
            date_str = date.strftime(cfg.TIME_FMT)
            names.append(f'{date_str}_Test File {i}.{ext}')
            date = datetime.now() - timedelta(days=random.randint(0, 10))
        return names

    def test_cleanup(self):
        now_dt = datetime(year=2020, month=4, day=9, hour=0, minute=0)
        # Adjust gap when running in the future
        gap = datetime.now() - now_dt
        gap = 7 + gap.days
        print(f'Using {gap} days gap')
        # Assumed gap of 7 days
        expected_def = {
            '200409-2100_Manual Entry 1.flv': False,
            '200409-2000_Manual Entry 2.flv': False,
            '200408-0133_Manual Entry 3.flv': False,
            '200403-2135_Manual Entry 4.flv': False,
            '200401-1849_Manual Entry 5.flv': True,
            '200327-1747_Manual Entry 6.flv': True,
        }
        for file, expected in expected_def.items():
            actual = utils.should_clean(file, days=gap)
            self.assertEqual(expected, actual, file)

    def test_cleanup_random(self):
        for file in self.generate_names(num_files=10):
            actual = utils.should_clean(file, days=7)
            print(f'Delete {file}? {actual}')


if __name__ == '__main__':
    unittest.main()
