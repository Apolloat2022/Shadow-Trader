import pandas as pd

# Load the local Gold data
df = pd.read_parquet('gold_features.parquet') 
latest = df.iloc[-1]

print(f"\n--- SHADOW TRADER SIGNAL TEST ---")
print(f"Date:          {latest['date']}")
# Changed 'close' to 'price' to match your Gold features
print(f"Current Price: ${latest['price']:.2f}")
print(f"7-Day Average: ${latest['sma_7']:.2f}")

if latest['price'] > latest['sma_7']:
    print("STATUS: 🟢 BULLISH (Trend is Up)")
    print("ACTION: BUY/HOLD")
else:
    print("STATUS: 🔴 BEARISH (Trend is Down)")
    print("ACTION: SELL/STAY OUT")
