import time
import subprocess
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

WATCH_DIRECTORY = Path("/mnt/i/Peder/repo/test_images")

# Inference script command (adjust to your script)
INFERENCE_COMMAND = ["echo", "ny bild i mappen"] # TODO hur vill vi anropa Williams modell (modeller)

class ImageHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            filepath = Path(event.src_path)
            if filepath.suffix.lower() in [".jpg", ".jpeg", ".png", ".bmp"]:
                print(f"[INFO] New image detected: {filepath}") # TODO ADD LOGGING INSTEAD
                self.run_inference(filepath)

    def run_inference(self, filepath: Path):
        try:
            print(f"[INFO] Running inference on {filepath}") # TODO ADD LOGGING INSTEAD
            subprocess.run(INFERENCE_COMMAND + [str(filepath)], check=True)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Inference failed: {e}") # TODO ADD LOGGING INSTEAD

if __name__ == "__main__":
    watch_dir = Path(WATCH_DIRECTORY)
    if not watch_dir.exists():
        raise FileNotFoundError(f"Watch directory {watch_dir} does not exist")

    event_handler = ImageHandler()
    observer = Observer()
    observer.schedule(event_handler, str(watch_dir), recursive=False)

    print("started")
    observer.start()

    try: # TODO daemon/service istället för loop som kräver startad terminal
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[INFO] Stopping observer...")
        observer.stop()

    observer.join()
