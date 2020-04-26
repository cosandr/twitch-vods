import os
import platform
import random
import unittest
from datetime import datetime, timedelta

import config as cfg
import utils

_base_video_path = 'media/twitch/RichardLewisReports'
_base_raw_path = 'downloads/twitch'
if platform.system() == 'Windows':
    _path_prefix = "Z:/"
else:
    _path_prefix = "/tank/"
    if platform.node().lower() != 'dresrv':
        _path_prefix = f"/mnt/sshfs{_path_prefix}"

VIDEO_PATH = f"{_path_prefix}{_base_video_path}"
RAW_VIDEO_PATH = f"{_path_prefix}{_base_raw_path}"


class TestCrop(unittest.TestCase):
    @staticmethod
    def generate_names(num_files=5, ext='flv'):
        """Generate file names according to format in config"""
        date = datetime.now()
        names = []
        for i in range(num_files):
            date_str = date.strftime(cfg.TIME_FMT)
            names.append(f'{date_str}_Test File {i}.{ext}')
            date = datetime.now() - timedelta(days=random.randint(0, 10), hours=random.randint(0, 10))
        return names

    def test_cleanup(self):
        test_dt = datetime(year=2020, month=4, day=9, hour=0, minute=0)
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
            actual = utils.should_clean(file, days=7, ref_dt=test_dt)
            self.assertEqual(expected, actual, file)

    def test_cleanup_random(self):
        del_str = "Random cleanup to be deleted:\n"
        nop_str = "Random cleanup no action:\n"
        for file in self.generate_names(num_files=10):
            actual = utils.should_clean(file, days=7)
            if actual:
                del_str += f"- {file}\n"
            else:
                nop_str += f"- {file}\n"
        print(del_str, nop_str)

    def test_check_for_deletion(self):
        test_dt = datetime(year=2020, month=4, day=9, hour=0, minute=0)
        # Assumed gap of 7 days
        # None - no action (date 2020-04-04 or higher)
        # False - warning (date 2020-04-03)
        # True - deleted (date 2020-04-02 or lower)
        expected_def = {
            '200409-2100_Manual Entry NOP.flv': None,
            '200409-2000_Manual Entry NOP.flv': None,
            '200408-0133_Manual Entry NOP.flv': None,
            '200403-0001_Manual Entry WARN.flv': None,
            '200403-0000_Manual Entry WARN.flv': None,
            '200402-2359_Manual Entry WARN.flv': False,
            '200402-0400_Manual Entry WARN.flv': False,
            '200402-0001_Manual Entry WARN.flv': False,
            '200402-0000_Manual Entry WARN.flv': False,
            '200401-2359_Manual Entry DEL.flv': True,
            '200401-1849_Manual Entry DEL.flv': True,
            '200327-1747_Manual Entry DEL.flv': True,
        }
        warn_list, del_list, _ = utils.check_for_deletion(list(expected_def.keys()), days=7, ref_dt=test_dt)
        for k, v in expected_def.items():
            if v is None:
                self.assertNotIn(k, warn_list)
                self.assertNotIn(k, del_list)
            elif v is True:
                self.assertNotIn(k, warn_list)
                self.assertIn(k, del_list)
            elif v is False:
                self.assertIn(k, warn_list)
                self.assertNotIn(k, del_list)

    def test_check_for_deletion_random(self):
        names = self.generate_names(num_files=10)
        warn_list, del_list, _ = utils.check_for_deletion(names, days=7)
        warn_str = "Random will be deleted tomorrow:\n"
        del_str = "Random deleted just now:\n"
        for n in warn_list:
            warn_str += f"- {n}\n"
        for n in del_list:
            del_str += f"- {n}\n"
        if del_list:
            print(del_str)
        if warn_list:
            print(warn_str)

    def test_check_for_deletion_actual(self):
        names = sorted(os.listdir(RAW_VIDEO_PATH))
        warn_list, del_list, _ = utils.check_for_deletion(names, days=4)
        warn_str = "Actual will be deleted tomorrow:\n"
        del_str = "Actual deleted just now:\n"
        for n in warn_list:
            warn_str += f"- {n}\n"
        for n in del_list:
            del_str += f"- {n}\n"
        if del_list:
            print(del_str)
        if warn_list:
            print(warn_str)


if __name__ == '__main__':
    unittest.main()
