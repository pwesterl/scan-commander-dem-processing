import time
import logging
import signal
import sys
from pathlib import Path
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler

from db_utils import ImageRepository, Status

WATCH_DIRECTORY = Path("/skog-nas01/scan-data/AW_bearbetning_test")
VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif"}
DB_INSERT_STATUS = Status.UNPROCESSED
POLL_INTERVAL = 1  # seconds

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s",
    "%Y-%m-%d %H:%M:%S"
)

fh = logging.FileHandler("logs/watcher.log", mode="a")
fh.setFormatter(formatter)

logger.addHandler(fh)

class ImageHandler(FileSystemEventHandler):
    """Handles new image files appearing in the watch directory."""
    def __init__(self, repo: ImageRepository):
        self.repo = repo

    def on_created(self, event) -> None:
        if event.is_directory:
            return

        filepath = Path(event.src_path)

        # Only accept files ending with /2_dtm/dtm.tif
        if not (filepath.name.lower() == "dtm.tif" and filepath.parent.name == "2_dtm"):
            logging.debug(f"Ignored file (not /2_dtm/dtm.tif): {filepath}")
            return

        try:
            logging.info(f"New valid image detected: {filepath}, inserting into DB")
            self.repo.insert_image(filepath, status=DB_INSERT_STATUS)
        except Exception:
            logging.exception(f"Failed to insert {filepath} into DB")



def main() -> None:
    if not WATCH_DIRECTORY.exists():
        raise FileNotFoundError(f"Watch directory {WATCH_DIRECTORY} does not exist")

    repo = ImageRepository()
    event_handler = ImageHandler(repo)
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
