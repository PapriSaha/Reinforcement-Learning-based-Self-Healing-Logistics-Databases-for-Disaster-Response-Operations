import os
import numpy as np
import pandas as pd
from stable_baselines3 import DQN
from stable_baselines3.common.vec_env import DummyVecEnv
from database_healing_env_realistic_v3 import RealisticDatabaseHealingEnvV3
from tqdm import tqdm
from datetime import datetime

# Load the trained model
MODEL_PATH = "dqn_self_healing_model_realistic_v3"
assert os.path.exists(MODEL_PATH + ".zip"), "Trained model not found!"
model = DQN.load(MODEL_PATH)

# Initialize environment
env = DummyVecEnv([lambda: RealisticDatabaseHealingEnvV3()])

# Evaluation loop
print("\U0001F680 Starting Real-Time Evaluation Loop")
total_episodes = 30
mttr_list = []
uptime_flags = []
anomaly_total = 0
anomaly_resolved = 0
uptime_total_steps = 0
successful_uptime_steps = 0

for _ in tqdm(range(total_episodes), desc="\U0001F50D Evaluating Agent"):
    obs = env.reset()
    done = False
    steps = 0
    episode_recovery_times = []
    episode_uptime = True

    while not done:
        action, _ = model.predict(obs, deterministic=True)
        obs, reward, done_flags, infos = env.step(action)

        done = done_flags[0]
        info = infos[0]  # Extract single environment info
        steps += 1

        if info.get("anomaly_present"):
            anomaly_total += 1
            if info.get("anomaly_resolved"):
                anomaly_resolved += 1
                episode_recovery_times.append(info.get("recovery_time", 0))

        if not info.get("uptime", True):
            episode_uptime = False
        else:
            successful_uptime_steps += 1

        uptime_total_steps += 1

    # MTTR
    if episode_recovery_times:
        mttr_list.append(np.mean(episode_recovery_times))

    # ZDSR
    uptime_flags.append(episode_uptime)

# Final Metrics
mean_mttr = np.mean(mttr_list) if mttr_list else float("inf")
zdsr = np.mean(uptime_flags)
resolution_accuracy = anomaly_resolved / anomaly_total if anomaly_total > 0 else 0
uptime_ratio = successful_uptime_steps / uptime_total_steps if uptime_total_steps > 0 else 0

print("\n\U0001F4CA Evaluation Summary")
print(f"Mean Time to Recovery (MTTR): {mean_mttr}")
print(f"Zero Downtime Success Rate (ZDSR): {zdsr}")
print(f"Anomaly Resolution Accuracy: {resolution_accuracy}")
print(f"24-Hour Uptime Ratio: {uptime_ratio}")

# Save results
results_df = pd.DataFrame([{
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "mean_time_to_recovery": mean_mttr,
    "zero_downtime_success_rate": zdsr,
    "anomaly_resolution_accuracy": resolution_accuracy,
    "uptime_ratio": uptime_ratio
}])

csv_path = "evaluation_metrics_realistic_v3.csv"
results_df.to_csv(csv_path, index=False)
print(f"✅ Results saved to {csv_path}")
