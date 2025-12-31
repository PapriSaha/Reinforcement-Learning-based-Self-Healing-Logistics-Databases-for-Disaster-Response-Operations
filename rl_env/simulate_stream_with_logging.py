
"""
Script: simulate_stream_with_logging.py
Purpose: Simulate real-time data streaming from `spill_incidents`
and log RL agent actions and rewards to a CSV file for analysis.

Author: Khondaker Zahin Fuad
"""

import time
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from stable_baselines3 import DQN
from database_healing_env import DatabaseHealingEnv
from datetime import datetime

# --- CONFIGURATION ---
DB_URL = "postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db"
MODEL_PATH = "./dqn_self_healing_model"
STREAM_TABLE = "stream_buffer"
LOG_FILE = "./agent_log.csv"

# --- SETUP ---
print("🔄 Loading data from `spill_incidents`...")
engine = create_engine(DB_URL)
df = pd.read_sql("SELECT * FROM spill_incidents ORDER BY spill_date ASC LIMIT 100", engine)

# --- RESET STREAM TABLE ---
with engine.begin() as conn:
    conn.execute(text(f"""
        DROP TABLE IF EXISTS {STREAM_TABLE};
        CREATE TABLE {STREAM_TABLE} AS TABLE spill_incidents WITH NO DATA;
    """))

# --- INIT ENV + MODEL ---
print("🧠 Loading trained agent and environment...")
env = DatabaseHealingEnv()
model = DQN.load(MODEL_PATH)

# --- INIT LOGGING ---
log_data = []

# --- STREAM LOOP ---
print("🚀 Beginning streaming simulation with logging...")
for i, row in df.iterrows():
    print(f"📡 Inserting row {i + 1}/{len(df)} into stream_buffer...")

    # Sanitize values for SQL insert
    sanitized = {
        f"val_{j}": (
            v.to_pydatetime() if isinstance(v, pd.Timestamp) and not pd.isna(v)
            else None if pd.isna(v)
            else v
        )
        for j, v in enumerate(row.values)
    }

    with engine.begin() as conn:
        insert_query = text(f"""
            INSERT INTO {STREAM_TABLE} ({', '.join(row.index)})
            VALUES ({', '.join([':val_' + str(j) for j in range(len(row))])})
        """)
        conn.execute(insert_query, sanitized)

    # Agent acts
    obs, _ = env.reset()
    action, _states = model.predict(obs, deterministic=True)
    reward = env.step(action)[1]
    action_name = ["Impute", "Deduplicate", "Rollback", "Index", "Plan"][action]

    print(f"🤖 Agent Action: {action_name} | Reward: {reward}\n")

    log_data.append({
        "timestamp": datetime.utcnow().isoformat(),
        "spill_number": row["spill_number"],
        "action_id": action,
        "action_name": action_name,
        "reward": reward,
        "state": obs.tolist()
    })

    time.sleep(0.5)

# --- SAVE LOG ---
log_df = pd.DataFrame(log_data)
log_df.to_csv(LOG_FILE, index=False)
print(f"✅ Log saved to {LOG_FILE}")
