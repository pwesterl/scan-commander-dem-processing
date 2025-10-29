from pathlib import Path

# Path to your AW_bearbetning folder
base_path = Path("/app/data/AW_bearbetning")

# Get all Areal directories and extract numbers
areal_dirs = [d.name for d in base_path.iterdir() if d.is_dir() and d.name.startswith("Areal")]
areal_numbers = sorted(int(d[5:]) for d in areal_dirs)  # remove "Areal" prefix

# Find missing numbers
max_number = max(areal_numbers)
missing = [n for n in range(1, max_number + 1) if n not in areal_numbers]

print(f"Missing Areal numbers ({len(missing)}):")
print(missing)
print(len(missing))
