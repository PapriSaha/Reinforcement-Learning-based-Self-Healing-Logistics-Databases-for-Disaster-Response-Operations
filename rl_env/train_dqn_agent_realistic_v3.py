import os
import numpy as np
from stable_baselines3 import DQN
from stable_baselines3.common.vec_env import DummyVecEnv
from database_healing_env_realistic_v3 import RealisticDatabaseHealingEnvV3

# Create environment
env = DummyVecEnv([lambda: RealisticDatabaseHealingEnvV3()])

# Create DQN agent
model = DQN(
    "MlpPolicy",
    env,
    verbose=1,
    learning_rate=1e-4,
    buffer_size=10000,
    learning_starts=1000,
    batch_size=32,
    tau=1.0,
    gamma=0.99,
    train_freq=4,
    target_update_interval=1000,
    exploration_fraction=0.2,
    exploration_final_eps=0.02,
    policy_kwargs=dict(net_arch=[256, 256]),
    tensorboard_log="./tensorboard_logs"
)

# Train the model
model.learn(total_timesteps=10000)

# Save the model
model.save("dqn_self_healing_model_realistic_v3")
print("✅ Model trained and saved.")
