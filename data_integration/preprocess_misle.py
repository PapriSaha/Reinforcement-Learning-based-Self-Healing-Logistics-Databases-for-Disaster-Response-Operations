"""
Preprocess and upload the MISLE dataset into PostgreSQL.
Final bug-free version with full datetime and key handling.
"""

import pandas as pd
from sqlalchemy import create_engine, MetaData, text
from datetime import datetime

# --- CONFIGURATION ---
EXCEL_PATH = "../data/misle_data.xlsx"  # Update as needed
DB_URL = "postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db"

# --- LOAD DATA ---
print("🔄 Loading dataset...")
df = pd.read_excel(EXCEL_PATH)
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# --- RENAME COLUMNS TO MATCH DB ---
df = df.rename(columns={"activityid": "activity_id"})

# --- ENSURE PRIMARY KEY IS PRESENT ---
if "activity_id" not in df.columns:
    raise ValueError("❌ Column 'activity_id' is missing. Cannot proceed.")

# --- CLEAN PRIMARY KEY + DROP DUPLICATES ---
df = df.dropna(subset=["activity_id"])
df = df.drop_duplicates(subset="activity_id", keep="first")

# --- CLEAN TIMESTAMPS ---
if "inspection_date" in df.columns:
    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    df["inspection_date"] = df["inspection_date"].astype(object)
    df["inspection_date"] = df["inspection_date"].apply(
        lambda x: x.to_pydatetime() if pd.notnull(x) else None
    )

# --- REPLACE ALL NaNs/NaTs WITH None ---
df = df.where(pd.notnull(df), None)

# --- SANITIZE EACH RECORD ---
def sanitize_record(row):
    clean = {}
    for key, value in zip(df.columns, row):
        if isinstance(value, pd._libs.tslibs.nattype.NaTType) or pd.isna(value):
            clean[key] = None
        elif isinstance(value, pd.Timestamp):
            clean[key] = value.to_pydatetime()
        else:
            clean[key] = value
    return clean

print("🔄 Creating sanitized records...")
records = [sanitize_record(row) for row in df.itertuples(index=False, name=None)]

# --- UPLOAD TO POSTGRESQL ---
print("🚀 Uploading to PostgreSQL...")
engine = create_engine(DB_URL)
metadata = MetaData()
metadata.reflect(bind=engine)
misle_table = metadata.tables.get("misle_reports")

if misle_table is None:
    print("❌ Table 'misle_reports' does not exist.")
else:
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT activity_id FROM misle_reports"))
        existing_ids = set(row[0] for row in existing.fetchall())

    before = len(records)
    records = [r for r in records if r["activity_id"] not in existing_ids]
    print(f"✅ {len(records)} new records to insert ({before - len(records)} skipped).")

    with engine.begin() as conn:
        conn.execute(misle_table.insert(), records)

    print("✅ Upload complete.")