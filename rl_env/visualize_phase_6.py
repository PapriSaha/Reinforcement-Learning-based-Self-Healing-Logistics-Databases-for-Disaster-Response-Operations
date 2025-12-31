import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load evaluation metrics CSV
df = pd.read_csv("evaluation_metrics_realistic_v4.csv", parse_dates=["timestamp"])

# Compute mean for each metric
summary = {
    "Mean Time to Recovery (MTTR)": df["mean_time_to_recovery"].mean(),
    "Zero Downtime Success Rate": df["zero_downtime_success_rate"].mean(),
    "Anomaly Resolution Accuracy": df["anomaly_resolution_accuracy"].mean(),
    "24-Hour Uptime Ratio": df["uptime_ratio"].mean()
}

# Display as a table
summary_df = pd.DataFrame([summary])
print(summary_df.to_string(index=False))

# Optionally save it
summary_df.to_csv("phase_6_result_summary.csv", index=False)

# Set a nicer style
sns.set(style="whitegrid")

# Plot: Mean Time to Recovery (MTTR)
plt.figure(figsize=(10, 6))
sns.lineplot(x="timestamp", y="mean_time_to_recovery", data=df, marker="o")
plt.title("📈 Mean Time to Recovery (MTTR) Over Time")
plt.ylabel("MTTR (seconds)")
plt.xlabel("Timestamp")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("mttr_over_time.png")
plt.show()

# Plot: Anomaly Resolution Accuracy
plt.figure(figsize=(10, 6))
sns.lineplot(x="timestamp", y="anomaly_resolution_accuracy", data=df, marker="o", color="green")
plt.title("✅ Anomaly Resolution Accuracy Over Time")
plt.ylabel("Accuracy")
plt.xlabel("Timestamp")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("anomaly_accuracy_over_time.png")
plt.show()

# Plot: Uptime Ratio
plt.figure(figsize=(10, 6))
sns.lineplot(x="timestamp", y="uptime_ratio", data=df, marker="o", color="orange")
plt.title("📶 Uptime Ratio Over Time")
plt.ylabel("Ratio")
plt.xlabel("Timestamp")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("uptime_ratio_over_time.png")
plt.show()

# Plot: Zero Downtime Success Rate (ZDSR)
plt.figure(figsize=(10, 6))
sns.lineplot(x="timestamp", y="zero_downtime_success_rate", data=df, marker="o", color="purple")
plt.title("🚫 Zero Downtime Success Rate Over Time")
plt.ylabel("ZDSR")
plt.xlabel("Timestamp")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("zdsr_over_time.png")
plt.show()

