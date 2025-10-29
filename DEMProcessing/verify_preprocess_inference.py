#!/usr/bin/env python3
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import DictCursor
from tabulate import tabulate

# ---------------- CONFIG ----------------
DATA_ROOT = Path("/app/data/AW_bearbetning")  # Root path to your Areal directories
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

def check_preprocess_files(conn):
    print("🔍 Checking preprocess_status files...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT source_path, comment
        FROM preprocess_status
        WHERE status IN ('preprocessed')
        AND source_path ~ '/AW_bearbetning/[0-9]{4}/(Areal|Area)[0-9]+/2_dtm/dtm\.tif$';
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

def check_inference_files(conn):
    print("\n🔍 Checking inference_status files...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT input_path, model_name
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
    conn = connect_db()
    try:
        check_preprocess_files(conn)
        check_inference_files(conn)
        print_orphan_areals(conn)
        check_dtm_file_sizes()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
