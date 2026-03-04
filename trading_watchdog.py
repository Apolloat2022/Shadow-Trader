import pandas as pd
import os

# 1. Load the data
if not os.path.exists('gold_features.parquet'):
    print("❌ Error: gold_features.parquet not found. Run gold_features.py first.")
    exit()

df = pd.read_parquet('gold_features.parquet')
latest = df.iloc[-1]
current_signal = "BULLISH" if latest['price'] > latest['sma_7'] else "BEARISH"

# 2. Check for signal change
state_file = 'last_signal.txt'
last_signal = ""
if os.path.exists(state_file):
    with open(state_file, 'r') as f:
        last_signal = f.read().strip()

# 3. Notification Logic
print(f"\n--- WATCHDOG STATUS: {current_signal} ---")
print(f"Price: ${latest['price']:.2f} | SMA: ${latest['sma_7']:.2f}")

if current_signal != last_signal:
    print(f"🚨 SIGNAL FLIP DETECTED! From {last_signal} to {current_signal}")
    # Update the state
    with open(state_file, 'w') as f:
        f.write(current_signal)
    # This is where you would trigger an email or SMS alert
else:
    print("✅ No change in signal. Steady as she goes.")
