#!/usr/bin/env python3
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import DictCursor
from tabulate import tabulate
import re
from tabulate import tabulate
import csv

# ---------------- CONFIG ----------------
DATA_ROOT = Path("/app/data/AW_bearbetning/")  # Root path to your Areal directories
DATA_ROOT_LOCAL = Path("/skog-nas01/scan-data/AW_bearbetning/")
DB_HOST = "db"        # Database host
DB_PORT = 5432        # Database port
DB_NAME = "geoint"    # Database name
DB_USER = "geouser"   # Database user
DB_PASSWORD = "geopass"  # Database password
# ----------------------------------------

def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def check_preprocess_files_from_db(conn):
    print("🔍 Checking preprocess_status files...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT source_path, comment
        FROM preprocess_status
        WHERE status IN ('preprocessed')
        AND source_path ~ '/AW_bearbetning_2025/(Areal|Area)[0-9]+/2_dtm/dtm\.tif$';
    """)
    rows = cursor.fetchall()
    missing = []

    for source_path, comment in rows:
        areal_dir = Path(source_path).parent.parent
        areal_name = areal_dir.name.lower()
        path_10cm = areal_dir / "preprocessed_10cm" / f"{areal_name}_preprocessed_10cm.tif"
        path_20cm = areal_dir / "preprocessed_20cm" / f"{areal_name}_preprocessed_20cm.tif"

        if not path_10cm.exists():
            missing.append(("preprocessed_10cm", path_10cm))
        if not path_20cm.exists():
            missing.append(("preprocessed_20cm", path_20cm))

    if missing:
        print(tabulate(missing, headers=["Preprocess Type", "Missing File"]))
    else:
        print("✅ All preprocess files exist")

def check_inference_files_from_db(conn):
    print("\n🔍 Checking inference_status files...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT source_path, model_name
        FROM inference_status
        WHERE status IN ('processed')
        AND input_path ~ '/AW_bearbetning/[0-9]{4}/(Areal|Area)[0-9]+/2_dtm/dtm\.tif$';
    """)
    rows = cursor.fetchall()
    missing = []

    for input_path, model_name in rows:
        output_dir = Path(input_path).parent.parent / "inference_output" / model_name
        if not output_dir.exists() or not any(output_dir.iterdir()):
            missing.append((model_name, output_dir))

    if missing:
        print(tabulate(missing, headers=["Model Name", "Missing Inference Output"]))
    else:
        print("✅ All inference outputs exist")


def check_preprocess_files_from_disc(
    area_paths,
    output_csv="missing_preprocessed_dtm.csv",
    output_txt="missing_preprocessed_dtm.txt",
    existing_txt="existing_preprocessed_dtm.txt"
):
    """
    Check if required preprocessed DTM directories exist and contain files.
    Writes missing and existing paths to separate files.
    """
    preprocessed_dirs = ["aggregated_20cm", "preprocessed_10cm", "preprocessed_20cm"]
    missing = []
    missing_paths = set()
    existing_paths = set()

    for path in area_paths:
        base_dir = Path(path).parent.parent

        # Track existence for all required dirs
        existing_all = True
        for preprocessed_dir in preprocessed_dirs:
            output_dir = base_dir / preprocessed_dir
            if not output_dir.exists() or not any(output_dir.iterdir()):
                existing_all = False
                missing.append((preprocessed_dir, str(output_dir)))
                missing_paths.add(str(path))

        if existing_all:
            existing_paths.add(str(path))

    # --- Output missing ones ---
    if missing:
        print(tabulate(missing, headers=["Preprocess Type", "Missing Directory"]))

        # Write CSV
        output_csv_path = Path(output_csv)
        with output_csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Preprocess Type", "Missing Directory"])
            writer.writerows(missing)

        # Write TXT (paths missing something)
        output_txt_path = Path(output_txt)
        with output_txt_path.open("w", encoding="utf-8") as f:
            for missing_path in sorted(missing_paths):
                f.write(missing_path + "\n")

        print(f"⚠️ Missing preprocess directories written to:\n  {output_csv_path.resolve()}\n  {output_txt_path.resolve()}")
    else:
        print("✅ All preprocessed DTM directories exist for all paths")

    # --- Output existing ones ---
    if existing_paths:
        existing_txt_path = Path(existing_txt)
        with existing_txt_path.open("w", encoding="utf-8") as f:
            for existing_path in sorted(existing_paths):
                f.write(existing_path + "\n")
        print(f"✅ Existing preprocess paths written to:\n  {existing_txt_path.resolve()}")
    else:
        print("⚠️ No fully preprocessed paths found.")

    return {
        "missing": missing,
        "missing_paths": missing_paths,
        "existing_paths": existing_paths,
    }


def check_inference_files_from_disc(area_paths, output_csv="missing_inference_outputs.csv", output_txt="missing_inference_outputs.txt"):
    models = ["fangstgrop", "kolbotten"]
    output_types = ["raster", "vector"]
    missing = []
    missing_paths = set()  # avoid duplicates

    for path in area_paths:
        base_dir = Path(path).parent.parent / "inference_output"
        for model in models:
            for output_type in output_types:
                output_dir = base_dir / model / output_type
                if not output_dir.exists() or not any(output_dir.iterdir()):
                    missing.append((f"{model}_{output_type}", str(output_dir)))
                    missing_paths.add(str(path))

    if missing:
        print(tabulate(missing, headers=["Model Output Type", "Missing Directory"]))

        # Write CSV
        output_csv_path = Path(output_csv)
        with output_csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Model Output Type", "Missing Directory"])
            writer.writerows(missing)

        # Write TXT (original area paths)
        output_txt_path = Path(output_txt)
        with output_txt_path.open("w", encoding="utf-8") as f:
            for missing_path in sorted(missing_paths):
                f.write(missing_path + "\n")

        print(f"⚠️ Missing inference outputs written to:\n  {output_csv_path.resolve()}\n  {output_txt_path.resolve()}")
    else:
        print("✅ All inference outputs exist")

    return missing

def print_orphan_areals(conn):
    """
    List all Areal directories on disk that do not have any corresponding entry in the database.
    """
    print("\n🔍 Checking for Areal directories on disk not registered in DB...")

    # Get all Areal names from the DB
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT substring(source_path from '.*/(Areal[0-9]+)/.*') AS areal_name
        FROM preprocess_status
        UNION
        SELECT DISTINCT substring(input_path from '.*/(Areal[0-9]+)/.*') AS areal_name
        FROM inference_status;
    """)
    db_areals = {row[0] for row in cursor.fetchall() if row[0]}

    # List all Areal directories on disk
    disk_areals = {d.name for d in DATA_ROOT.iterdir() if d.is_dir() and d.name.lower().startswith("areal")}

    # Compare and print missing
    orphan_areals = sorted(disk_areals - db_areals)
    if orphan_areals:
        print(f"⚠️ {len(orphan_areals)} Areal directories exist on disk but not in DB:\n")
        for areal in orphan_areals:
            print(f" - {areal}")
    else:
        print("✅ All Areal directories on disk are registered in the DB.")

def check_dtm_file_sizes():
    """
    Traverse all directories under DATA_ROOT and report the size of dtm.tif files
    located in any 2_dtm subdirectory.
    """
    print("\n🔍 Checking dtm.tif file sizes...")
    dtm_files = list(DATA_ROOT.rglob("2_dtm/dtm.tif"))
    if not dtm_files:
        print("⚠️ No dtm.tif files found.")
        return

    dtm_sizes = []
    for dtm_file in dtm_files:
        if dtm_file.is_file():
            size_mb = dtm_file.stat().st_size / (1024 * 1024)  # bytes → MB
            dtm_sizes.append((dtm_file, size_mb))

    # Print table of file sizes
    print(tabulate(dtm_sizes, headers=["dtm.tif File", "Size (MB)"], floatfmt=".2f"))

    # Print total size
    total_size = sum(size for _, size in dtm_sizes)
    print(f"\n📦 Total size of all dtm.tif files: {total_size:.2f} MB")


def main():
    print("🔎 Verifying preprocess and inference files on disk...")
    pattern = re.compile(
        r".*[/\\]AW_bearbetning[^/\\]*[/\\](?:\d{4}[/\\])?Area[l]?[^/\\]+[/\\]2_dtm[/\\]dtm\.tif$",
        re.IGNORECASE
    )
    print("🧭 Scanning disk for dtm.tif files...")
    dtm_paths = []
    for dirpath, _, filenames in os.walk(DATA_ROOT):
        for filename in filenames:
            if filename.lower() == "dtm.tif":
                full_path = os.path.join(dirpath, filename)
                if pattern.match(full_path):
                    dtm_paths.append(Path(full_path).resolve())
    print(f"🧭 Found {len(dtm_paths)} matching dtm.tif files on disk.")

    #conn = connect_db()
    #try:
    check_inference_files_from_disc(dtm_paths)
    check_preprocess_files_from_disc(dtm_paths)
        #check_preprocess_files_from_db(conn)
        #check_inference_files_from_db(conn)
        #print_orphan_areals(conn)
        #check_dtm_file_sizes()
    #finally:
     #   conn.close()

if __name__ == "__main__":
    main()
