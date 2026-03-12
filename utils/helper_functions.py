import re
from pathlib import Path

def get_areal_id(path: Path):
    match = re.search(r"/(areal\d+|area\d+)/", str(path), re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find Areal ID in path: {path}")
    return match.group(1).lower()