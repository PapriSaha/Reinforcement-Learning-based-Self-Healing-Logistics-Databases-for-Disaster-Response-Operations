
"""
Script: label_spill_anomalies.py
Purpose: Identify basic anomalies in `spill_incidents` and store binary flag
in column `spill_anomaly_flag`.

Rules:
- Missing zip_code
- spill_date > close_date
- recovered > quantity
- duration (close_date - spill_date) > 30 days
"""

import pandas as pd
from sqlalchemy import create_engine, MetaData, text
from datetime import timedelta

# --- CONFIG ---
DB_URL = "postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db"

# --- LOAD DATA ---
print("🔄 Loading spill_incidents from database...")
engine = create_engine(DB_URL)
df = pd.read_sql("SELECT * FROM spill_incidents", engine)

# --- CLEAN TIMESTAMPS ---
print("🧹 Parsing date columns...")
df["spill_date"] = pd.to_datetime(df["spill_date"], errors="coerce")
df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")

# --- DEFINE ANOMALY CONDITIONS ---
print("🔎 Evaluating anomaly conditions...")
df["duration_days"] = (df["close_date"] - df["spill_date"]).dt.days
df["spill_anomaly_flag"] = 0

df.loc[df["zip_code"].isna(), "spill_anomaly_flag"] = 1
df.loc[df["recovered"] > df["quantity"], "spill_anomaly_flag"] = 1
df.loc[df["spill_date"] > df["close_date"], "spill_anomaly_flag"] = 1
df.loc[df["duration_days"] > 30, "spill_anomaly_flag"] = 1

df["spill_anomaly_flag"] = df["spill_anomaly_flag"].fillna(0).astype(int)

# --- WRITE TO DATABASE ---
print("🚀 Writing anomaly flags to database...")
with engine.begin() as conn:
    # Ensure column exists
    # conn.execute(text("""
    #     ALTER TABLE spill_incidents
    #     ADD COLUMN IF NOT EXISTS spill_anomaly_flag INTEGER
    # """))

    for _, row in df.iterrows():
        conn.execute(
            text("""
                UPDATE spill_incidents
                SET spill_anomaly_flag = :flag
                WHERE spill_number = :id
            """),
            {"flag": row["spill_anomaly_flag"], "id": row["spill_number"]}
        )

print("✅ Anomaly labels added to spill_incidents.")
