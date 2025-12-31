
"""
Script: visualize_agent_log.py
Purpose: Visualize agent behavior from `agent_log.csv` using matplotlib and pandas.

Creates:
- Cumulative reward line chart
- Action distribution bar chart
- State variable trends over time

Author: Khondaker Zahin Fuad
"""

import pandas as pd
import matplotlib.pyplot as plt

# --- Load Log ---
log_path = "./agent_log.csv"
df = pd.read_csv(log_path)

# --- Cumulative Reward Plot ---
df["cumulative_reward"] = df["reward"].cumsum()
plt.figure(figsize=(10, 5))
plt.plot(df["timestamp"], df["cumulative_reward"], marker='o')
plt.xticks(rotation=45)
plt.title("Cumulative Reward Over Time")
plt.xlabel("Time")
plt.ylabel("Cumulative Reward")
plt.tight_layout()
plt.savefig("reward_over_time.png")
plt.close()

# --- Action Frequency Bar Chart ---
plt.figure(figsize=(8, 4))
df["action_name"].value_counts().plot(kind="bar", color="skyblue")
plt.title("Agent Action Distribution")
plt.xlabel("Action")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig("action_distribution.png")
plt.close()

# --- State Trends (Optional Subplots) ---
states = df["state"].apply(eval).tolist()
state_df = pd.DataFrame(states, columns=[
    "missing_pct", "anomaly_count", "query_latency",
    "act1", "act2", "act3", "act4", "act5"
])
state_df["timestamp"] = df["timestamp"]

plt.figure(figsize=(10, 6))
plt.plot(state_df["timestamp"], state_df["missing_pct"], label="Missing %")
plt.plot(state_df["timestamp"], state_df["anomaly_count"], label="Anomaly Count")
plt.plot(state_df["timestamp"], state_df["query_latency"], label="Latency")
plt.xticks(rotation=45)
plt.title("Key State Metrics Over Time")
plt.xlabel("Time")
plt.ylabel("Value")
plt.legend()
plt.tight_layout()
plt.savefig("state_metrics_over_time.png")
plt.close()

print("✅ Plots saved: reward_over_time.png, action_distribution.png, state_metrics_over_time.png")
