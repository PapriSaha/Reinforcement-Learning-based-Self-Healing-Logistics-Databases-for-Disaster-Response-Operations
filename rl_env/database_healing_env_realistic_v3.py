
import gymnasium as gym
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from gymnasium import spaces

class RealisticDatabaseHealingEnvV3(gym.Env):
    def __init__(self):
        super(RealisticDatabaseHealingEnvV3, self).__init__()

        self.observation_space = spaces.Box(
            low=np.array([0.0, 0.0, 0.0] + [0.0]*5),
            high=np.array([1.0, 100.0, 1000.0] + [1.0]*5),
            dtype=np.float32
        )
        self.action_space = spaces.Discrete(5)
        self.max_steps = 30

        self.engine = create_engine("postgresql+psycopg2://logistics_user:admin@localhost:5432/logistics_db")
        self.conn = self.engine.connect()

        self.reset()

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)

        query = text("SELECT * FROM stream_buffer ORDER BY RANDOM() LIMIT 1")
        row = self.conn.execute(query).mappings().fetchone()

        self.anomaly_present = bool(row['spill_anomaly_flag'])
        self.anomaly_resolved = False
        self.recovery_time = 0
        self.steps = 0
        self.uptime = True

        missing_pct = np.random.uniform(0.1, 0.4) if self.anomaly_present else np.random.uniform(0.0, 0.1)
        anomaly_count = np.random.uniform(10, 30) if self.anomaly_present else np.random.uniform(0, 5)
        query_time = np.random.uniform(300, 600) if self.anomaly_present else np.random.uniform(50, 150)

        self.state = np.array([missing_pct, anomaly_count, query_time] + [0]*5, dtype=np.float32)
        return self.state, {}

    def step(self, action):
        missing, anomalies, query_time = self.state[:3]
        action_hist = self.state[3:]

        reward = 0
        self.steps += 1
        info = {}

        if action == 0:  # Impute missing
            if missing > 0.05:
                missing -= 0.1
                reward += 1
            else:
                reward -= 4
        elif action == 1:  # Remove duplicates
            if anomalies > 0:
                anomalies -= 1
                reward += 1
            else:
                reward -= 4
        elif action == 2:  # Rollback
            if self.anomaly_present:
                self.anomaly_present = False
                self.anomaly_resolved = True
                self.recovery_time += 1
                reward += 5
            else:
                reward -= 10
        elif action == 3:  # Optimize indexing
            if query_time > 100:
                query_time -= np.random.uniform(30, 60)
                reward += 2
            else:
                reward -= 3
        elif action == 4:  # Reconfigure query plan
            if query_time > 100:
                query_time -= np.random.uniform(20, 50)
                reward += 1
            else:
                reward -= 4

        self.recovery_time += 1
        self.uptime = query_time < 100

        action_hist = np.roll(action_hist, -1)
        action_hist[-1] = 1

        self.state = np.array([
            max(0.0, missing),
            max(0.0, anomalies),
            max(0.0, query_time),
            *action_hist
        ], dtype=np.float32)

        terminated = self.steps >= self.max_steps
        truncated = False

        info.update({
            "anomaly_present": self.anomaly_present,
            "anomaly_resolved": self.anomaly_resolved,
            "recovery_time": self.recovery_time if self.anomaly_resolved else 0,
            "uptime": self.uptime
        })

        return self.state, reward, terminated, truncated, info
