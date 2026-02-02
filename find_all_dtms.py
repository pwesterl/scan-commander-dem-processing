import os
import re
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def find_dtm_files(data_root: Path):
    """Find all dtm.tif files under AW_bearbetningX/(year)/Area(l)*/2_dtm/dtm.tif"""
    pattern = re.compile(
        r".*[/\\]AW_bearbetning[^/\\]*[/\\](?:\d{4}[/\\])?Area[l]?[^/\\]+[/\\]2_dtm[/\\]dtm\.tif$",
        re.IGNORECASE
    )

    dtm_paths = []
    for dirpath, _, filenames in os.walk(data_root):
        for filename in filenames:
            if filename.lower() == "dtm.tif":
                full_path = os.path.join(dirpath, filename)
                if pattern.match(full_path):
                    dtm_paths.append(Path(full_path).resolve())

    logger.info(f"🧭 Found {len(dtm_paths)} matching dtm.tif files on disk.")
    return dtm_paths


if __name__ == "__main__":
    DATA_ROOT = Path("/app/data/AW_bearbetning_2025")

    dtm_files = find_dtm_files(DATA_ROOT)

    logger.info("📂 List of found dtm.tif files:")
    for path in dtm_files:
        print(path)

    # Optional: write them to a text file
    output_file = Path("2025dtm.txt")
    with output_file.open("w", encoding="utf-8") as f:
        for p in dtm_files:
            f.write(str(p) + "\n")

    logger.info(f"💾 Paths saved to: {output_file.resolve()}")
