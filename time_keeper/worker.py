import signal
import sys
import time
from pathlib import Path
from . import db


class Worker:
    def __init__(self, db_path: Path, interval_seconds: float = 1.0):
        self.db_path = db_path
        self.interval = float(interval_seconds)
        self._running = True

    def _handle_stop(self, *_):
        self._running = False

    def run(self):
        signal.signal(signal.SIGINT, self._handle_stop)
        try:
            signal.signal(signal.SIGTERM, self._handle_stop)
        except Exception:
            pass
        print("Time Keeper worker started. Press Ctrl+C to stop.")
        ticks = 0
        while self._running:
            updated, deactivated = db.deduct_one_second_all_active(self.db_path)
            ticks += 1
            if ticks % 10 == 0:
                print(f"tick={ticks} updated={updated} deactivated={deactivated}")
            time.sleep(self.interval)
        print("Worker stopped.")


def run(db_path: Path, interval_seconds: float = 1.0):
    Worker(db_path, interval_seconds).run()
