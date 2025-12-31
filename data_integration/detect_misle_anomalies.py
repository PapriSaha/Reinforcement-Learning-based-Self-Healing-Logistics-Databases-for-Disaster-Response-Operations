"""
Script: detect_misle_anomalies.py
Purpose: Apply TF-IDF + Isolation Forest to detect textual anomalies
in the `description_clean` field of the `misle_reports` table.

Stores anomaly scores back in `description_anomaly_score` column.

Author: Khondaker Zahin Fuad
"""

import pandas as pd
from sqlalchemy import create_engine, MetaData, text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import IsolationForest

# --- CONFIGURATION ---
DB_URL = "postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db"

# --- LOAD DATA ---
print("🔄 Loading cleaned descriptions...")
engine = create_engine(DB_URL)
df = pd.read_sql("SELECT activity_id, description_clean FROM misle_reports", engine)

# --- DROP EMPTY DESCRIPTIONS ---
df = df.dropna(subset=["description_clean"])

# --- TF-IDF VECTORIZATION ---
print("🔠 Vectorizing descriptions using TF-IDF...")
vectorizer = TfidfVectorizer(max_features=500, stop_words="english")
X = vectorizer.fit_transform(df["description_clean"])

# --- ISOLATION FOREST ---
print("🌲 Running Isolation Forest...")
model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
df["description_anomaly_score"] = model.fit_predict(X)

# Convert output: -1 (outlier) → 1, 1 (inlier) → 0
df["description_anomaly_score"] = df["description_anomaly_score"].apply(lambda x: 1 if x == -1 else 0)

# --- WRITE BACK TO DATABASE ---
print("🚀 Writing anomaly scores to database...")
with engine.begin() as conn:
    # Ensure column exists
    # conn.execute(text("""
    #     ALTER TABLE misle_reports
    #     ADD COLUMN IF NOT EXISTS description_anomaly_score INTEGER
    # """))

    for _, row in df.iterrows():
        conn.execute(
            text("""
                UPDATE misle_reports
                SET description_anomaly_score = :score
                WHERE activity_id = :id
            """),
            {"score": int(row["description_anomaly_score"]), "id": row["activity_id"]}
        )

print("✅ Anomaly scores stored successfully.")
