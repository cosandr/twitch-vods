import asyncio
import platform
import re
from datetime import datetime, timedelta

import pytest_check as check

import utils
from modules import Cleaner

_base_video_path = 'media/twitch/RichardLewisReports'
_base_raw_path = 'downloads/twitch'
if platform.system() == 'Windows':
    _path_prefix = "Z:/"
else:
    _path_prefix = "/tank/"
    if platform.node().lower() != 'dresrv':
        _path_prefix = f"/dresrv{_path_prefix}"

TIME_FMT = '%y%m%d-%H%M'
VIDEO_PATH = f"{_path_prefix}{_base_video_path}"
RAW_VIDEO_PATH = f"{_path_prefix}{_base_raw_path}"


class TestCleanup:
    @classmethod
    def setup_class(cls):
        cls.loop = asyncio.get_event_loop()
        cls.cleaner = Cleaner(loop=cls.loop, check_path=RAW_VIDEO_PATH, enable_notifications=False, dry_run=True)

    def test_show_actual(self):
        self.cleaner.update()
        warn_str = self.cleaner.check_for_warnings()
        del_str = self.cleaner.delete_pending()
        print(warn_str)
        print(del_str)

    def test_delete_pending(self):
        test_dt = datetime(year=2020, month=4, day=1, hour=0, minute=0)
        input_files = {
            '200328-0000_DEL.flv',  # Already deleted
            '200329-1900_DEL.flv',  # Already deleted
            '200330-2100_DEL.flv',  # Already deleted
            '200401-0001_SOON.flv',  # 1 min
            '200401-0100_SOON.flv',  # 1 hour
            '200401-0600_WARN.flv',  # 6 hours
            '200401-1600_WARN.flv',  # 16 hours
            '200403-0000_WARN.flv',  # 2 days
        }
        self.cleaner.pending.clear()
        for n in input_files:
            self.cleaner.pending[n] = utils.get_datetime(n, TIME_FMT)
        expected_def = [
            {
                "add_hours": 0,  # Reference time 2020-04-01 00:00
                "pending": [
                    '200401-0001_SOON.flv',
                    '200401-0100_SOON.flv',
                    '200401-0600_WARN.flv',
                    '200401-1600_WARN.flv',
                    '200403-0000_WARN.flv',
                ],
                "del_str": [
                    '200328-0000_DEL',
                    '200329-1900_DEL',
                    '200330-2100_DEL',
                ],
            },
            {
                "add_hours": 0,  # Reference time 2020-04-01 00:00
                "pending": [
                    '200401-0001_SOON.flv',
                    '200401-0100_SOON.flv',
                    '200401-0600_WARN.flv',
                    '200401-1600_WARN.flv',
                    '200403-0000_WARN.flv',
                ],
                "del_str": [],
            },
            {
                "add_hours": 4,  # Reference time 2020-04-01 04:00
                "pending": [
                    '200401-0600_WARN.flv',
                    '200401-1600_WARN.flv',
                    '200403-0000_WARN.flv',
                ],
                "del_str": [
                    '200401-0001_SOON',
                    '200401-0100_SOON',
                ],
            },
            {
                "add_hours": 24,  # Reference time 2020-04-02 04:00
                "pending": [
                    '200403-0000_WARN.flv',
                ],
                "del_str": [
                    '200401-0600_WARN',
                    '200401-1600_WARN',
                ],
            },
            {
                "add_hours": 24,  # Reference time 2020-04-03 04:00
                "pending": [],
                "del_str": [
                    '200403-0000_WARN',
                ],
            },
        ]
        is_in_err = []
        re_deleted = re.compile(r'-\s\"(.*)\".*')
        ref_dt = test_dt
        for expected in expected_def:
            ref_dt += timedelta(hours=expected['add_hours'])
            del_str = self.cleaner.delete_pending(ref_dt=ref_dt)
            print(f"\n--- Current time: {str(ref_dt)} ---\n{del_str}")
            check.equal(sorted(expected['pending']), sorted(self.cleaner.pending.keys()), str(ref_dt))

            actual_str = []
            for m in re_deleted.finditer(del_str):
                actual_str.append(m.group(1))
            # Make sure output string contains what it should
            for name in expected["del_str"]:
                check.is_in(name, actual_str, f"MISSING {name} - {ref_dt}")
                if name not in actual_str:
                    is_in_err.append(f"MISSING {name} - {ref_dt}")
            # Make sure there are no extras
            for name in actual_str:
                check.is_in(name, expected["del_str"], f"EXTRA {name} - {ref_dt}")
                if name not in expected["del_str"]:
                    is_in_err.append(f"EXTRA {name} - {ref_dt}")
        if is_in_err:
            print(f"\nExpected in del_str but not found:\n" + '\n'.join(is_in_err))

    def test_check_for_warnings(self):
        test_dt = datetime(year=2020, month=4, day=1, hour=0, minute=0)
        input_files = {
            '200330-2100_DEL.flv',  # Already deleted
            '200401-0001_TOO SOON.flv',  # 1 min
            '200401-0100_TOO SOON.flv',  # 1 hour
            '200401-0101_WARN.flv',  # 1 hour, 1 minute
            '200401-0600_WARN.flv',  # 6 hours
            '200401-1201_WARN.flv',  # 12 hours, 1 minute
            '200401-1600_WARN.flv',  # 16 hours
            '200401-2300_WARN.flv',  # 23 hours
            '200402-0000_WARN.flv',  # 24 hours
            '200402-1200_WARN.flv',  # 1 day, 12 hours
            '200403-0000_WARN.flv',  # 2 days
            '200403-1200_WARN.flv',  # 2 days, 12 hours
            '200404-0000_WARN.flv',  # 3 days
            '200405-0000_WARN.flv',  # 4 days
            '200406-0000_WARN.flv',  # 5 days
        }
        self.cleaner.pending.clear()
        for n in input_files:
            self.cleaner.pending[n] = utils.get_datetime(n, TIME_FMT)

        expected_warn = [
            {
                "add_hours": 0,
                "warned": {
                    '200401-0001_TOO SOON.flv': 12,  # 1 min
                    '200401-0100_TOO SOON.flv': 12,  # 1 hour
                    '200401-0101_WARN.flv': 12,  # 1 hour, 1 minute
                    '200401-0600_WARN.flv': 12,  # 6 hours
                    '200401-1201_WARN.flv': 24,  # 12 hours, 1 minute
                    '200401-1600_WARN.flv': 24,  # 16 hours
                    '200401-2300_WARN.flv': 24,  # 23 hours
                    '200402-0000_WARN.flv': 24,  # 24 hours
                    '200402-1200_WARN.flv': 48,  # 1 day, 12 hours
                    '200403-0000_WARN.flv': 48,  # 2 days
                },
                "warn_str": [
                    '200401-0001_TOO SOON',
                    '200401-0100_TOO SOON',
                    '200401-0101_WARN',
                    '200401-0600_WARN',
                    '200401-1201_WARN',
                    '200401-1600_WARN',
                    '200401-2300_WARN',
                    '200402-0000_WARN',
                    '200402-1200_WARN',
                    '200403-0000_WARN',
                ],
            },
            {
                "add_hours": 0,
                "warned": {
                    '200401-0001_TOO SOON.flv': 12,  # 1 min
                    '200401-0100_TOO SOON.flv': 12,  # 1 hour
                    '200401-0101_WARN.flv': 12,  # 1 hour, 1 minute
                    '200401-0600_WARN.flv': 12,  # 6 hours
                    '200401-1201_WARN.flv': 24,  # 12 hours, 1 minute
                    '200401-1600_WARN.flv': 24,  # 16 hours
                    '200401-2300_WARN.flv': 24,  # 23 hours
                    '200402-0000_WARN.flv': 24,  # 24 hours
                    '200402-1200_WARN.flv': 48,  # 1 day, 12 hours
                    '200403-0000_WARN.flv': 48,  # 2 days
                },
                "warn_str": [],
            },
            {
                "add_hours": 3,  # Reference time 2020-04-01 03:00
                "warned": {
                    '200401-0600_WARN.flv': 12,  # 3 hours
                    '200401-1201_WARN.flv': 12,  # 9 hours, 1 minute
                    '200401-1600_WARN.flv': 24,  # 13 hours
                    '200401-2300_WARN.flv': 24,  # 20 hours
                    '200402-0000_WARN.flv': 24,  # 21 hours
                    '200402-1200_WARN.flv': 48,  # 1 day, 9 hours
                    '200403-0000_WARN.flv': 48,  # 1 day, 21 hours
                },
                "warn_str": [
                    '200401-1201_WARN',
                ],
            },
            {
                "add_hours": 6,  # Reference time 2020-04-01 09:00
                "warned": {
                    '200401-1201_WARN.flv': 12,  # 3 hours, 1 minute
                    '200401-1600_WARN.flv': 12,  # 7 hours
                    '200401-2300_WARN.flv': 24,  # 14 hours
                    '200402-0000_WARN.flv': 24,  # 15 hours
                    '200402-1200_WARN.flv': 48,  # 1 day, 3 hours
                    '200403-0000_WARN.flv': 48,  # 1 day, 15 hours
                },
                "warn_str": [
                    '200401-1600_WARN',
                ],
            },
            {
                "add_hours": 17,  # Reference time 2020-04-02 02:00
                "warned": {
                    '200402-1200_WARN.flv': 12,  # 10 hours
                    '200403-0000_WARN.flv': 24,  # 22 hours
                    '200403-1200_WARN.flv': 48,  # 1 day, 10 hours
                    '200404-0000_WARN.flv': 48,  # 1 day, 22 hours
                },
                "warn_str": [
                    '200402-1200_WARN',
                    '200403-0000_WARN',
                    '200403-1200_WARN',
                    '200404-0000_WARN',
                ],
            },
            {
                "add_hours": 22,  # Reference time 2020-04-03 00:00
                "warned": {
                    '200403-1200_WARN.flv': 12,  # 12 hours
                    '200404-0000_WARN.flv': 24,  # 1 day
                    '200405-0000_WARN.flv': 48,  # 2 days
                },
                "warn_str": [
                    '200403-1200_WARN',
                    '200404-0000_WARN',
                    '200405-0000_WARN',
                ],
            },
            {
                "add_hours": 36,  # Reference time 2020-04-04 12:00
                "warned": {
                    '200405-0000_WARN.flv': 12,  # 12 hours
                    '200406-0000_WARN.flv': 48,  # 1 day, 12 hours
                },
                "warn_str": [
                    '200405-0000_WARN',
                    '200406-0000_WARN',
                ],
            },
            {
                "add_hours": 48,  # Reference time 2020-04-06 12:00
                "warned": {},
                "warn_str": [],
            },
        ]
        is_in_err = []
        re_deleted = re.compile(r'-\s\"(.*)\".*')
        ref_dt = test_dt
        for expected in expected_warn:
            ref_dt += timedelta(hours=expected['add_hours'])
            warn_str = self.cleaner.check_for_warnings(ref_dt=ref_dt)
            print(f"\n--- Current time: {str(ref_dt)} ---\n{warn_str}")
            for actual_name, actual_th in self.cleaner.warned.items():
                check.equal(expected['warned'].get(actual_name), actual_th, f"{actual_name} - {ref_dt}")

            actual_str = []
            for m in re_deleted.finditer(warn_str):
                actual_str.append(m.group(1))
            # Make sure output string contains what it should
            for name in expected["warn_str"]:
                check.is_in(name, actual_str, f"MISSING {name} - {ref_dt}")
                if name not in actual_str:
                    is_in_err.append(f"MISSING {name} - {ref_dt}")
            # Make sure there are no extras
            for name in actual_str:
                check.is_in(name, expected["warn_str"], f"EXTRA {name} - {ref_dt}")
                if name not in expected["warn_str"]:
                    is_in_err.append(f"EXTRA {name} - {ref_dt}")
        if is_in_err:
            print(f"\nExpected in warn_str but not found:\n" + '\n'.join(is_in_err))
