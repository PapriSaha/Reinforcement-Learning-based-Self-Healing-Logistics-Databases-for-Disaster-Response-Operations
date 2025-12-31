"""
Script: clean_misle_descriptions.py
Purpose: Cleans and tokenizes the `description` field in the MISLE dataset,
then updates the PostgreSQL table `misle_reports` with a new column `description_clean`.

Author: Khondaker Zahin Fuad
"""

import pandas as pd
import re
import nltk
from sqlalchemy import create_engine, MetaData, text

# Optional: uncomment if running for the first time
nltk.download('punkt')
nltk.download('stopwords')

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# --- CONFIGURATION ---
DB_URL = "postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db"

# --- CONNECT TO POSTGRESQL AND LOAD DATA ---
print("🔄 Loading descriptions from database...")
engine = create_engine(DB_URL)
query = "SELECT activity_id, description FROM misle_reports"
df = pd.read_sql(query, engine)

# --- CLEAN + TOKENIZE DESCRIPTION ---
stop_words = set(stopwords.words('english'))

def clean_and_tokenize(text):
    if not isinstance(text, str):
        return None
    text = text.lower()
    text = re.sub(r"_x000d_|\n|\r", " ", text)
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)
    tokens = word_tokenize(text)
    tokens = [word for word in tokens if word not in stop_words and len(word) > 2]
    return " ".join(tokens)

print("🧹 Cleaning and tokenizing descriptions...")
df["description_clean"] = df["description"].apply(clean_and_tokenize)

# --- UPLOAD CLEANED DATA BACK TO POSTGRES ---
print("🚀 Updating database with cleaned descriptions...")
with engine.begin() as conn:
    # Add new column if it doesn't exist
    # conn.execute(text("""
    #     ALTER TABLE misle_reports
    #     ADD COLUMN IF NOT EXISTS description_clean TEXT
    # """))

    # Update each row
    for _, row in df.iterrows():
        conn.execute(
            text("""
                UPDATE misle_reports
                SET description_clean = :desc
                WHERE activity_id = :id
            """),
            {"desc": row["description_clean"], "id": row["activity_id"]}
        )

print("✅ Descriptions cleaned and updated in database.")
