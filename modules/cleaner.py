import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import List, Tuple, Dict

from utils import get_datetime, setup_logger, human_timedelta, fmt_plural_str
from .notifier import Notifier


class Cleaner:
    """
    pending = {
        <file_path>: <datetime, deletion time>
    }
    <datetime of deletion time> ==> file parsed time/modified time + clean_days (default 7)
    warned = {
        <file_path>: <int, hours warning>
    }
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, check_path: str, log_parent='', clean_days=7,
                 warn_at: List[int] = None, notifier: Notifier = None, enable_notifications=True,
                 dry_run=False):
        self.loop = loop
        self.dry_run = dry_run
        self.check_path: str = check_path
        self.clean_days: int = clean_days
        if warn_at:
            self.warn_at: List[int] = sorted(warn_at, reverse=True)
        else:
            self.warn_at: List[int] = [48, 24, 12]
        self.warn_at.insert(0, 2*self.warn_at[0])
        # --- Logger ---
        logger_name = self.__class__.__name__
        if log_parent:
            logger_name = f'{log_parent}.{logger_name}'
        self.logger: logging.Logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        setup_logger(self.logger, 'cleaner')
        self.notifier: Notifier = notifier
        if enable_notifications and not self.notifier:
            self.notifier = Notifier(loop=self.loop, log_parent=logger_name)
        self.pending: Dict[str, datetime] = {}
        # Track for which files we sent warnings for and when
        self.warned: Dict[str, int] = {}
        # List of files which could not be parsed
        self.blacklist: List[str] = []
        self.update()
        # Event for running task again
        self.en_del = asyncio.Event()
        # noinspection PyTypeChecker
        # Task for wait task
        self.wait_task: asyncio.Task = None
        # noinspection PyTypeChecker
        # Task for worker
        self.worker_task: asyncio.Task = None
        if not self.dry_run:
            self.worker_task = self.loop.create_task(self.worker())
            self.en_del.set()
            self.loop.create_task(self.warning_worker())

    def close(self):
        for task in (self.wait_task, self.worker_task):
            if task:
                try:
                    task.cancel()
                except:
                    self.logger.exception("Failed to close task")

    async def send_notification(self, content: str):
        self.logger.debug(f"Sending notification\n{content}")
        if not self.notifier:
            return
        try:
            await self.notifier.send(content, name='Twitch Cleaner')
        except:
            self.logger.exception('Cannot send notification')

    async def worker(self):
        """Setting en_del externally will also force an update"""
        try:
            while True:
                await self.en_del.wait()
                # Try to cancel task if it is already running
                if self.wait_task:
                    try:
                        self.wait_task.cancel()
                    except:
                        self.logger.exception("Cannot cancel timer")
                        pass
                self.en_del.clear()
                # Update pending list
                self.update()
                self.wait_task = await self.wait_next_delete()
        except asyncio.CancelledError:
            self.logger.info("Worker task cancelled")

    async def warning_worker(self):
        while True:
            # Get warning and send notification
            warn_str = self.check_for_warnings()
            if warn_str:
                await self.send_notification(warn_str)
            # Run again in an hour
            await asyncio.sleep(3600)

    def update(self) -> None:
        """Updates pending"""
        names = self.get_files()
        self.pending.clear()
        for n in names:
            if n in self.blacklist:
                continue
            try:
                file_dt = get_datetime(n, self.check_path)
                if not isinstance(file_dt, datetime):
                    raise ValueError(f"Expected datetime, got {type(file_dt)}: {file_dt}")
                self.logger.info(f"{n} will be deleted at {file_dt}")
            except:
                self.blacklist.append(n)
                self.logger.exception(f"Cannot determine datetime for {n}")
                continue
            self.pending[n] = file_dt + timedelta(days=self.clean_days)

    async def wait_next_delete(self, ref_dt=None) -> None:
        """Delete pending and wait for next one"""
        # Delete pending first
        del_str = self.delete_pending(ref_dt=ref_dt)
        if del_str:
            await self.send_notification(del_str)
        # Don't do anything if we've deleted everything
        if not self.pending:
            return
        if not ref_dt:
            ref_dt = datetime.now()
        # Get item with closest datetime
        del_k = sorted(self.pending, key=self.pending.get)[0]
        del_dt = self.pending[del_k]
        del_delta = del_dt - ref_dt
        del_sec = del_delta.total_seconds()
        if del_sec < 0:
            return
        if del_sec > 3456000:
            self.logger.warning(f"Tried to sleep more than 3456000 second limit: {del_sec}")
            del_sec = 3456000
        self.logger.info(f"Next delete at {del_dt}, sleeping {del_sec / 3600:.1f} hours")
        try:
            await asyncio.sleep(del_sec)
            self.en_del.set()
        except asyncio.CancelledError:
            self.logger.debug("Timer cancelled")
            self.wait_task = None

    def delete_pending(self, ref_dt=None) -> str:
        """Deletes pending files, returns a status string of deleted items"""
        if not self.pending:
            return ""
        del_list = []
        err_list = []
        if not ref_dt:
            ref_dt = datetime.now()
        new_pending = self.pending.copy()
        for k, dt in sorted(self.pending.items()):
            if dt > ref_dt:
                continue
            if k in self.blacklist:
                continue
            name = os.path.splitext(k)[0]
            try:
                if not self.dry_run:
                    os.unlink(os.path.join(self.check_path, k))
                del_list.append(f"- \"{name}\"")
            except Exception as e:
                self.blacklist.append(k)
                err_list.append(f"- \"{name}\": {str(e)}")
                self.logger.exception(name)
            new_pending.pop(k, None)
        self.pending = new_pending
        ret_str = ""
        if del_list:
            ret_str = f"Deleted {fmt_plural_str(len(del_list))}:\n" + "\n".join(del_list)
        if err_list:
            if ret_str:
                ret_str += "\n"
            ret_str += f"Could not delete {fmt_plural_str(len(err_list))} videos:\n" + "\n".join(err_list)
        return ret_str

    def check_for_warnings(self, ref_dt=None) -> str:
        if not self.pending:
            return ""
        warn_list = []
        if not ref_dt:
            ref_dt = datetime.now()
        for k, dt in sorted(self.pending.items()):
            if k in self.blacklist:
                continue
            # Might be negative for old files
            del_td = dt - ref_dt
            del_hours = del_td.total_seconds() / 3600
            # Don't warn for negative values (will be or has been deleted already)
            if del_hours <= 0:
                self.warned.pop(k, None)
                continue
            # Don't warn if it's too far into the future
            if del_hours > self.warn_at[0]:
                continue
            # Determine warning threshold
            curr_th = self.warned.get(k, self.warn_at[0])
            next_th = None
            for th in self.warn_at[1:]:
                if th >= del_hours:
                    next_th = th

            # No more thresholds, no more warnings
            if next_th is None:
                continue
            if curr_th == next_th:
                continue
            self.warned[k] = next_th
            time_str = human_timedelta(del_td, max_vals=2)
            # Remove extension
            name = os.path.splitext(k)[0]
            warn_list.append(f"- \"{name}\" will be deleted in {time_str}")
        if warn_list:
            return f"{fmt_plural_str(len(warn_list))} to be deleted:\n" + "\n".join(warn_list)
        return ""

    def get_files(self, check_ext: Tuple[str] = (".flv",)) -> List[str]:
        check_files = []
        for f in os.listdir(self.check_path):
            if f in self.blacklist:
                continue
            _, ext = os.path.splitext(f)
            if ext in check_ext:
                check_files.append(f)
        return check_files