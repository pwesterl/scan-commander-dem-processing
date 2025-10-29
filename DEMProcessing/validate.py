#!/usr/bin/env python3
import psycopg2
from pathlib import Path
from tabulate import tabulate

# ---------------- CONFIG ----------------
DATA_ROOT = Path("/app/data/AW_bearbetning")
DB_HOST = "db"
DB_PORT = 5432
DB_NAME = "geoint"
DB_USER = "geouser"
DB_PASSWORD = "geopass"
# ----------------------------------------

def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def analyze_areals(conn):
    print("🔍 Gathering preprocess and inference status per Areal...")

    cur = conn.cursor()

    # --- Get preprocessed Areals ---
    cur.execute("""
        SELECT DISTINCT substring(source_path from '.*/(Areal[0-9]+)/.*') AS areal
        FROM preprocess_status
        WHERE status = 'preprocessed';
    """)
    preprocessed = {row[0] for row in cur.fetchall() if row[0]}

    # --- Get inferenced Areals ---
    cur.execute("""
        SELECT DISTINCT substring(input_path from '.*/(Areal[0-9]+)/.*') AS areal
        FROM inference_status
        WHERE status = 'processed';
    """)
    inferenced = {row[0] for row in cur.fetchall() if row[0]}

    # --- Get all Areals on disk ---
    on_disk = {d.name for d in DATA_ROOT.iterdir() if d.is_dir() and d.name.lower().startswith("areal")}

    # --- Categorize ---
    fully_done = sorted(preprocessed & inferenced)
    only_preprocessed = sorted(preprocessed - inferenced)
    missing_both = sorted(on_disk - preprocessed - inferenced)

    # --- Summary ---
    print("\n📊 Summary:")
    print(f"  ✅ Fully processed & inferenced: {len(fully_done)}")
    print(f"  ⚙️  Only preprocessed: {len(only_preprocessed)}")
    print(f"  ❌ Missing both: {len(missing_both)}")

    # --- Detailed listing ---
    print("\n✅ Fully processed & inferenced Areals:")
    for a in fully_done[:15]:
        print(f"  - {a}")
    if len(fully_done) > 15:
        print(f"  ... and {len(fully_done) - 15} more.")

    print("\n⚙️  Only preprocessed (not inferenced):")
    for a in only_preprocessed[:15]:
        print(f"  - {a}")
    if len(only_preprocessed) > 15:
        print(f"  ... and {len(only_preprocessed) - 15} more.")

    print("\n❌ Missing from both preprocess and inference:")
    for a in missing_both[:15]:
        print(f"  - {a}")
    if len(missing_both) > 15:
        print(f"  ... and {len(missing_both) - 15} more.")

def main():
    print("🔎 Checking Areal processing progress...")
    conn = connect_db()
    try:
        analyze_areals(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
