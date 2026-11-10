"""Run one study driver with cooperative cancellation so its finally blocks execute."""

from __future__ import annotations

import _thread
import argparse
import os
import runpy
import sys
import threading
from pathlib import Path

from eirvah_observability.experiment import emit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cancel", type=Path, required=True)
    parser.add_argument("script", type=Path)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    stop = threading.Event()

    def watch() -> None:
        while not stop.wait(0.25):
            if args.cancel.exists():
                _thread.interrupt_main()
                return

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    emit("process_started", name="driver", process_pid=os.getpid())
    sys.argv = [str(args.script), *args.arguments]
    try:
        runpy.run_path(str(args.script), run_name="__main__")
    finally:
        stop.set()
        thread.join(timeout=1)


if __name__ == "__main__":
    main()
