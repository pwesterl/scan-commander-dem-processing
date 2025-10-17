import time
import logging
import signal
import sys
import os
from pathlib import Path
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler

from db_utils import PreprocessRepository, Status

WATCH_DIRECTORY = Path(os.getenv("WATCH_DIR", "/skog-nas01/scan-data/AW_bearbetning_test"))
DB_INSERT_STATUS = Status.UNPROCESSED
POLL_INTERVAL = 1  # seconds

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s",
    "%Y-%m-%d %H:%M:%S"
)
Path("logs").mkdir(exist_ok=True)
fh = logging.FileHandler("logs/watcher.log", mode="a")
fh.setFormatter(formatter)

logger.addHandler(fh)

class ImageHandler(FileSystemEventHandler):
    """Handles new image files appearing in the watch directory."""
    def __init__(self, repo: PreprocessRepository,stable_wait: float = 5.0):
        self.repo = repo

    def is_file_stable(self, path: Path, check_interval=2, stable_count=3, max_wait=600) -> bool:
        start_time = time.time()
        last_size = -1
        stable_steps = 0
        while time.time() - start_time < max_wait:
            if not path.is_file():
                return False
            current_size = path.stat().st_size
            if current_size == last_size:
                stable_steps += 1
                if stable_steps >= stable_count:
                    return True
            else:
                stable_steps = 0
            last_size = current_size
            time.sleep(check_interval)

        logging.warning(f"File {path} never stabilized after {max_wait}s")
        return False
    
    def on_created(self, event) -> None:
        if event.is_directory:
            return

        filepath = Path(event.src_path)
        if not (filepath.name.lower() == "dtm.tif" and filepath.parent.name == "2_dtm"):
            logging.debug(f"Ignored file (not /2_dtm/dtm.tif): {filepath}")
            return

        if not self.is_file_stable(filepath):
            logging.info(f"File {filepath} is still being copied, will retry later")
            return

        try:
            logging.info(f"New valid image detected: {filepath}, inserting into DB")
            self.repo.insert_image(filepath, status=DB_INSERT_STATUS)
        except Exception:
            logging.exception(f"Failed to insert {filepath} into DB")



def main() -> None:
    if not WATCH_DIRECTORY.exists():
        raise FileNotFoundError(f"Watch directory {WATCH_DIRECTORY} does not exist")

    repo = PreprocessRepository(WATCH_DIRECTORY)
    event_handler = ImageHandler(repo, stable_wait=5.0)
    observer = Observer()
    observer.schedule(event_handler, str(WATCH_DIRECTORY), recursive=True) # Sätt true om vi vill kolla submappar
    observer.start()

    logging.info("=======")
    logging.info("Image Directory Watcher started")
    logging.info(f"Watching directory: {WATCH_DIRECTORY}")
    logging.info("=======")

    def shutdown_handler(sig, frame):
        logging.info("Stopping observer...")
        observer.stop()
        observer.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Keep alive loop
    try:
        while True:
            time.sleep(POLL_INTERVAL)
    except Exception:
        logging.exception("Unexpected error in main loop")
        shutdown_handler(None, None)


if __name__ == "__main__":
    main()
