import pandas as pd

def run_backtest(df, window, initial_balance=10000.0, fee_rate=0.001):
    df = df.copy()
    df['sma'] = df['price'].rolling(window=window).mean()
    df = df.dropna(subset=['sma']).reset_index(drop=True)
    df['daily_return'] = df['price'].pct_change()
    
    balance = initial_balance
    position = 0
    trades = 0
    
    for i in range(1, len(df)):
        price = df.iloc[i]['price']
        sma = df.iloc[i]['sma']
        daily_ret = df.iloc[i]['daily_return']
        
        if position == 1:
            balance *= (1 + daily_ret)
        
        if price > sma and position == 0:
            balance *= (1 - fee_rate)
            position = 1
            trades += 1
        elif price < sma and position == 1:
            balance *= (1 - fee_rate)
            position = 0
            trades += 1
    return balance, trades

df_gold = pd.read_parquet('gold_features.parquet')
df_gold['date'] = pd.to_datetime(df_gold['date'])
df_gold = df_gold.sort_values('date')

windows = [7, 20, 50, 200]
print(f"\n--- SHADOW TRADER: STRATEGY COMPARISON ---")
print(f"{'Window':<10} | {'Ending Balance':<20} | {'Trades':<10}")
print("-" * 55)

for w in windows:
    final_bal, total_trades = run_backtest(df_gold, w)
    label = f"{w} Day"
    print(f"{label:<10} | ${final_bal:19,.2f} | {total_trades:<10}")

# Buy and Hold Benchmark
first_price = df_gold.iloc[0]['price']
last_price = df_gold.iloc[-1]['price']
benchmark = 10000.0 * (last_price / first_price)
print("-" * 55)
print(f"BUY & HOLD | ${benchmark:19,.2f}")
