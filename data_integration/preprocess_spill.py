"""
Finalized script: Cleans and uploads the Spill Incident Records dataset to PostgreSQL
Author: Khondaker Zahin Fuad
"""

import pandas as pd
from sqlalchemy import create_engine, MetaData
from datetime import datetime
import numpy as np

# --- CONFIGURATION ---
CSV_PATH = "../data/spill_incidents.csv"  # Adjust if needed
DB_URL = "postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db"

# --- LOAD & INITIAL CLEANING ---
print("🔄 Loading dataset...")
df = pd.read_csv(CSV_PATH, low_memory=False)
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# Remove duplicate rows based on primary key (spill_number)
df = df.drop_duplicates(subset="spill_number", keep="first")


# --- CLEAN DATE COLUMNS ---
print("🧹 Cleaning data...")
date_cols = ["spill_date", "received_date", "close_date"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors="coerce")

# --- SANITIZE ALL MISSING VALUES ---
df = df.where(pd.notnull(df), None)  # replaces NaN, NaT, np.nan with None

print(f"✅ Cleaned {len(df)} records")

# --- SANITIZE ROWS FOR DATABASE INSERT ---
def sanitize_record(row):
    clean = {}
    for key, value in zip(df.columns, row):
        if isinstance(value, pd._libs.tslibs.nattype.NaTType):
            clean[key] = None
        elif pd.isna(value):
            clean[key] = None
        elif isinstance(value, pd.Timestamp):
            clean[key] = value.to_pydatetime()
        else:
            clean[key] = value
    return clean

print("🔄 Creating sanitized records...")
records = [sanitize_record(row) for row in df.itertuples(index=False, name=None)]

# --- CONNECT TO POSTGRESQL AND UPLOAD ---
print("🚀 Uploading to PostgreSQL...")
engine = create_engine(DB_URL)
metadata = MetaData()
metadata.reflect(bind=engine)
spill_table = metadata.tables.get("spill_incidents")

print("🔍 Checking for existing spill_numbers in the database...")
with engine.connect() as conn:
    from sqlalchemy import text
    result = conn.execute(text("SELECT spill_number FROM spill_incidents"))
    existing_ids = set(row[0] for row in result.fetchall())

# Filter out duplicates
initial_count = len(records)
records = [r for r in records if r["spill_number"] not in existing_ids]
print(f"✅ {len(records)} new records to insert ({initial_count - len(records)} skipped as duplicates).")


if spill_table is None:
    print("❌ Table 'spill_incidents' does not exist.")
else:
    with engine.begin() as conn:
        conn.execute(spill_table.insert(), records)
    print("✅ Upload complete — all NaT safely handled.")
