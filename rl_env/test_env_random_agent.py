
"""
Script: test_env_random_agent.py
Purpose: Run the DatabaseHealingEnv using random actions for basic validation.

Make sure database_healing_env.py is in the Python path.

Author: Khondaker Zahin Fuad
"""

import gym
import numpy as np
import sys
import os

# Ensure the custom environment can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database_healing_env import DatabaseHealingEnv

# --- Initialize Environment ---
env = DatabaseHealingEnv()
obs = env.reset()

print("\n🎮 Starting Random Agent Test")
print("-----------------------------")

total_reward = 0
for step in range(30):
    action = env.action_space.sample()
    obs, reward, done, _ = env.step(action)
    total_reward += reward

    print(f"Step {step+1}: Action={action}, Reward={reward}, State={obs.tolist()}")

    if done:
        print("\n✅ Environment reached terminal state (anomalies resolved).")
        break

print(f"🎯 Total Reward: {total_reward}")
env.close()
