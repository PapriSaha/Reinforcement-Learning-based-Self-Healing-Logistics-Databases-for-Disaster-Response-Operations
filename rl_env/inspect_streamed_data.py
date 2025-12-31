
import pandas as pd
from sqlalchemy import create_engine, text

# 🔧 Configure your database connection
DB_URI = "postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db"
engine = create_engine(DB_URI)

print("🔍 Connecting to database and fetching recent stream data...")
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM stream_buffer ORDER BY spill_number DESC LIMIT 50"))
    rows = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(rows, columns=columns)

print(f"📦 Retrieved {len(df)} records. Checking for anomalies...")

# Detect anomalies
anomalies = df[
    (df["spill_anomaly_flag"] == 1) |
    (df["quantity"] > 10000) |
    (df["material_name"].str.lower().str.contains("unknown", na=False)) |
    (df["spill_date"].isna()) |
    (df["recovered"].isna())
]

print(f"⚠️ Detected {len(anomalies)} anomalous records out of {len(df)}.")
if not anomalies.empty:
    print(anomalies[[
        "spill_number", "spill_date", "material_name", "quantity", "recovered", "spill_anomaly_flag"
    ]])
else:
    print("✅ No anomalies detected in the latest records.")
