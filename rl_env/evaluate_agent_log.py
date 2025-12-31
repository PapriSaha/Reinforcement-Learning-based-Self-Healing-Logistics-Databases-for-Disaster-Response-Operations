
"""
Script: evaluate_agent_log.py
Purpose: Compute performance metrics for the RL agent based on agent_log.csv.

Metrics:
- Mean Time to Recovery (MTTR)
- Zero Downtime Success Rate (ZDSR)
- Anomaly Resolution Accuracy
- Uptime Ratio

Author: Khondaker Zahin Fuad
"""

import pandas as pd
import numpy as np
import ast
import csv
import os
from datetime import datetime

# --- Load Log File ---
df = pd.read_csv("agent_log.csv")

# --- Parse State Vector from String to List ---
df["state"] = df["state"].apply(ast.literal_eval)

# Extract key state values
state_df = pd.DataFrame(df["state"].tolist(), columns=[
    "missing_pct", "anomaly_count", "query_latency", "act1", "act2", "act3", "act4", "act5"
])
df = pd.concat([df, state_df], axis=1)

# --- Metric 1: Mean Time to Recovery (MTTR) ---
recovery_steps = []
counter = 0
for count in df["anomaly_count"]:
    counter += 1
    if count == 0:
        recovery_steps.append(counter)
        counter = 0
if not recovery_steps:
    mttr = float("inf")
else:
    mttr = np.mean(recovery_steps)

# --- Metric 2: Zero Downtime Success Rate (ZDSR) ---
# Assume 'Rollback' = potential downtime
zdsr = 1 - (df["action_name"] == "Rollback").sum() / len(df)

# --- Metric 3: Anomaly Resolution Accuracy ---
# Check if action resulted in anomaly reduction at next step
correct = 0
total = 0
for i in range(len(df) - 1):
    if df.loc[i, "anomaly_count"] > df.loc[i+1, "anomaly_count"]:
        correct += 1
    total += 1
ara = correct / total if total > 0 else 0

# --- Metric 4: Uptime Ratio ---
# Define uptime as anomaly_count <= 5 and missing_pct <= 0.2
uptime_rows = df.query("anomaly_count <= 5 and missing_pct <= 0.2").shape[0]
uptime_ratio = uptime_rows / len(df)

# --- Save Results to CSV---
# Define the CSV log file path
csv_file_path = "evaluation_metrics_log.csv"

# Metrics dictionary (replace these with your real values after evaluation)
metrics = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "mean_time_to_recovery": mttr,
    "zero_downtime_success_rate": zdsr,
    "anomaly_resolution_accuracy": ara,
    "uptime_ratio": uptime_ratio
}

# Check if the file already exists
file_exists = os.path.isfile(csv_file_path)

# Write to CSV (append mode)
with open(csv_file_path, mode='a', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=metrics.keys())

    # Write header only once
    if not file_exists:
        writer.writeheader()

    # Write the current metrics
    writer.writerow(metrics)

# --- Print Results ---
print("📊 Evaluation Metrics (From agent_log.csv)")
print("------------------------------------------")
print(f"🕒 Mean Time to Recovery (MTTR): {mttr:.2f} steps")
print(f"⚙️  Zero Downtime Success Rate (ZDSR): {zdsr:.2%}")
print(f"🎯 Anomaly Resolution Accuracy: {ara:.2%}")
print(f"⏱️  Uptime Ratio: {uptime_ratio:.2%}")
