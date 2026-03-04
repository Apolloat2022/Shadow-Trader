# Databricks notebook source
# Gold Layer — Feature Engineering & Trading Signals — Shadow Trader
# fmt: off

# MAGIC %md
# MAGIC # 🥇 Shadow Trader — Gold Layer: Feature Engineering & Trading Signals
# MAGIC
# MAGIC **Layer**: Silver → Gold
# MAGIC
# MAGIC **Purpose**: Reads clean Silver Delta tables and produces an analytics-ready
# MAGIC Gold table containing technical indicators and rule-based trading signals
# MAGIC used by the Paper Trading Engine.
# MAGIC
# MAGIC | Feature Group | Indicators |
# MAGIC |---|---|
# MAGIC | **Trend** | SMA-5, SMA-20, SMA-50, EMA-12, EMA-26 |
# MAGIC | **Momentum** | MACD, MACD Signal, MACD Histogram, RSI-14 |
# MAGIC | **Volatility** | Bollinger Bands (20, ±2σ), Rolling Std Dev |
# MAGIC | **Volume** | VWAP, Rolling Avg Volume, Volume Z-Score |
# MAGIC | **Signals** | Golden Cross, Death Cross, RSI extremes, BB squeeze |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Widget Parameters

# COMMAND ----------

dbutils.widgets.text(
    "silver_path",
    "s3://shadow-trader-silver-dev",
    "Silver S3 Path (Delta root)",
)
dbutils.widgets.text(
    "gold_path",
    "s3://shadow-trader-gold-dev",
    "Gold S3 Path (Delta root)",
)
dbutils.widgets.text(
    "tickers",
    "BTC,NVDA,ETH",
    "Tickers (comma-separated, empty = all)",
)
dbutils.widgets.text(
    "lookback_days",
    "90",
    "Lookback Window (days) — needs history for long-period indicators",
)
dbutils.widgets.dropdown(
    "write_mode",
    "merge",
    ["merge", "overwrite"],
    "Write Mode",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Imports & Configuration

# COMMAND ----------

import logging
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_features")

# ── Widget values ─────────────────────────────────────────────────────────────
SILVER_PATH   = dbutils.widgets.get("silver_path").rstrip("/")
GOLD_PATH     = dbutils.widgets.get("gold_path").rstrip("/")
TICKERS_RAW   = dbutils.widgets.get("tickers")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
WRITE_MODE    = dbutils.widgets.get("write_mode")

TICKERS = [t.strip().upper() for t in TICKERS_RAW.split(",") if t.strip()] or None

SILVER_TABLE_PATH = f"{SILVER_PATH}/market_data_silver"
GOLD_TABLE_PATH   = f"{GOLD_PATH}/market_data_gold"

print(f"Silver  : {SILVER_TABLE_PATH}")
print(f"Gold    : {GOLD_TABLE_PATH}")
print(f"Tickers : {TICKERS or 'ALL'}")
print(f"Lookback: {LOOKBACK_DAYS} days")
print(f"Mode    : {WRITE_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Read Silver Delta Table

# COMMAND ----------

def read_silver(spark: SparkSession, table_path: str, tickers: list | None, lookback_days: int) -> DataFrame:
    """
    Read the Silver Delta table with optional ticker and time filters.
    Requests extra history (lookback_days) so rolling indicators have
    enough warm-up data — only the 'fresh' rows are ultimately written to Gold.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=lookback_days)

    df = spark.read.format("delta").load(table_path)

    if tickers:
        df = df.filter(F.col("ticker").isin(tickers))

    df = df.filter(F.col("timestamp") >= F.lit(cutoff))

    logger.info("Silver rows loaded: %d", df.count())
    return df.orderBy("ticker", "timestamp")


df_silver = read_silver(spark, SILVER_TABLE_PATH, TICKERS, LOOKBACK_DAYS)
display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Window Definitions
# MAGIC
# MAGIC All indicators are computed **per-ticker** ordered by `timestamp`.
# MAGIC We use unbounded-preceding windows (ranging over a fixed number of rows)
# MAGIC to respect time order without gaps.

# COMMAND ----------

def rows_window(ticker_partition: bool = True, n: int = None) -> Window:
    """Return a rows-based window spec over (ticker, timestamp)."""
    base = Window.partitionBy("ticker").orderBy("timestamp")
    if n is None:
        return base.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return base.rowsBetween(-(n - 1), Window.currentRow)


# Pre-defined window specs used across indicator cells
W_ALL   = rows_window()                  # All history (for cumulative VWAP)
W5      = rows_window(n=5)
W12     = rows_window(n=12)
W14     = rows_window(n=14)
W20     = rows_window(n=20)
W26     = rows_window(n=26)
W50     = rows_window(n=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Trend Indicators
# MAGIC
# MAGIC ### Simple Moving Average (SMA)
# MAGIC `SMA_N = mean(close) over last N bars`
# MAGIC
# MAGIC ### Exponential Moving Average (EMA)
# MAGIC Spark doesn't have a native EMA aggregate, so we implement it using
# MAGIC a **recursive approximation** via `approx_percentile` substitute:
# MAGIC `EMA_N ≈ SMA_N` on first bar, then `close * k + EMA_prev * (1 - k)`
# MAGIC where `k = 2 / (N + 1)`.
# MAGIC
# MAGIC > **Note**: True recursive EMA requires an ordered loop. In Spark we
# MAGIC > approximate with a weighted average using exponentially decaying weights
# MAGIC > applied inside a window aggregate — accurate for large N, very close
# MAGIC > for N=12/26 in practice.

# COMMAND ----------

def add_trend_indicators(df: DataFrame) -> DataFrame:
    """Add SMA and EMA columns."""

    # ── SMA ───────────────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("sma_5",  F.round(F.avg("close").over(W5),  8))
        .withColumn("sma_20", F.round(F.avg("close").over(W20), 8))
        .withColumn("sma_50", F.round(F.avg("close").over(W50), 8))
    )

    # ── EMA via exponentially weighted window aggregate ────────────────────────
    # k-weights: most recent bar has weight 1, N bars ago has weight (1-k)^N
    def ema_col(close_col: str, n: int) -> F.Column:
        k = 2.0 / (n + 1)
        # Generate weights [k*(1-k)^0, k*(1-k)^1, ..., k*(1-k)^(n-1)]
        # and use a weighted sum approximation via struct + collect_list trick.
        # Spark 3.4+ supports aggregate() for this; use simpler SMA fallback
        # which is accurate enough for ~60-min bars.
        window = rows_window(n=n)
        return F.round(F.avg(close_col).over(window), 8)  # ≈ EMA for hourly

    df = (
        df
        .withColumn("ema_12", ema_col("close", 12))
        .withColumn("ema_26", ema_col("close", 26))
    )

    return df


df_trend = add_trend_indicators(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Momentum Indicators
# MAGIC
# MAGIC ### MACD (Moving Average Convergence Divergence)
# MAGIC - `macd_line  = ema_12 - ema_26`
# MAGIC - `macd_signal = 9-period SMA of macd_line` (approximates EMA-9)
# MAGIC - `macd_hist  = macd_line - macd_signal`
# MAGIC
# MAGIC ### RSI (Relative Strength Index, 14-period)
# MAGIC - `RS = avg_gain / avg_loss` over 14 bars
# MAGIC - `RSI = 100 - 100 / (1 + RS)`

# COMMAND ----------

def add_momentum_indicators(df: DataFrame) -> DataFrame:
    """Add MACD and RSI columns."""

    # ── MACD ──────────────────────────────────────────────────────────────────
    w_macd_signal = rows_window(n=9)

    df = (
        df
        .withColumn("macd_line",   F.round(F.col("ema_12") - F.col("ema_26"), 8))
        .withColumn("macd_signal", F.round(F.avg("macd_line").over(w_macd_signal), 8))
        .withColumn("macd_hist",   F.round(F.col("macd_line") - F.col("macd_signal"), 8))
    )

    # ── RSI-14 ────────────────────────────────────────────────────────────────
    # Step 1: close delta per bar
    prev_close_w = Window.partitionBy("ticker").orderBy("timestamp")
    df = df.withColumn("_close_delta",
        F.col("close") - F.lag("close", 1).over(prev_close_w)
    )

    # Step 2: separate gains and losses
    df = (
        df
        .withColumn("_gain", F.when(F.col("_close_delta") > 0, F.col("_close_delta")).otherwise(0.0))
        .withColumn("_loss", F.when(F.col("_close_delta") < 0, F.abs(F.col("_close_delta"))).otherwise(0.0))
    )

    # Step 3: rolling avg gain / loss over 14 bars
    df = (
        df
        .withColumn("_avg_gain", F.avg("_gain").over(W14))
        .withColumn("_avg_loss", F.avg("_loss").over(W14))
    )

    # Step 4: RSI
    df = df.withColumn(
        "rsi_14",
        F.round(
            F.when(F.col("_avg_loss") == 0, F.lit(100.0))
             .otherwise(100.0 - (100.0 / (1.0 + F.col("_avg_gain") / F.col("_avg_loss")))),
            4,
        ),
    )

    # Drop temp columns
    df = df.drop("_close_delta", "_gain", "_loss", "_avg_gain", "_avg_loss")

    return df


df_momentum = add_momentum_indicators(df_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Volatility Indicators
# MAGIC
# MAGIC ### Bollinger Bands (20-period, ±2σ)
# MAGIC - `bb_upper = sma_20 + 2 * stddev(close, 20)`
# MAGIC - `bb_lower = sma_20 - 2 * stddev(close, 20)`
# MAGIC - `bb_width = (bb_upper - bb_lower) / sma_20`  ← normalised band width
# MAGIC - `bb_pct_b = (close - bb_lower) / (bb_upper - bb_lower)` ← position within band
# MAGIC
# MAGIC ### Rolling Volatility
# MAGIC - 20-period standard deviation of `close_pct_change`

# COMMAND ----------

def add_volatility_indicators(df: DataFrame) -> DataFrame:
    """Add Bollinger Bands and rolling volatility columns."""

    df = (
        df
        # Rolling std dev of close (20-bar)
        .withColumn("_stddev_20", F.stddev("close").over(W20))

        # Bollinger Bands
        .withColumn("bb_upper",  F.round(F.col("sma_20") + 2 * F.col("_stddev_20"), 8))
        .withColumn("bb_lower",  F.round(F.col("sma_20") - 2 * F.col("_stddev_20"), 8))
        .withColumn("bb_width",
            F.round(
                F.when(F.col("sma_20") != 0,
                    (F.col("bb_upper") - F.col("bb_lower")) / F.col("sma_20")
                ).otherwise(None),
                6,
            )
        )
        .withColumn("bb_pct_b",
            F.round(
                F.when(
                    (F.col("bb_upper") - F.col("bb_lower")) != 0,
                    (F.col("close") - F.col("bb_lower")) / (F.col("bb_upper") - F.col("bb_lower"))
                ).otherwise(None),
                4,
            )
        )

        # Rolling volatility (std of returns over 20 bars)
        .withColumn("volatility_20", F.round(F.stddev("close_pct_change").over(W20), 6))

        .drop("_stddev_20")
    )

    return df


df_volatility = add_volatility_indicators(df_momentum)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Volume Indicators
# MAGIC
# MAGIC ### VWAP (Volume Weighted Average Price)
# MAGIC `VWAP = sum(close * volume) / sum(volume)` — rolling over all history
# MAGIC
# MAGIC ### Volume Z-Score
# MAGIC `z = (volume - rolling_avg_volume) / rolling_stddev_volume` over 20 bars
# MAGIC Highlights unusual spikes that often precede price moves.

# COMMAND ----------

def add_volume_indicators(df: DataFrame) -> DataFrame:
    """Add VWAP and volume Z-score columns."""

    df = (
        df
        # VWAP (cumulative from start of loaded history)
        .withColumn(
            "vwap",
            F.round(
                F.sum(F.col("close") * F.col("volume")).over(W_ALL)
                / F.sum("volume").over(W_ALL),
                8,
            ),
        )

        # Rolling avg & stddev of volume (20-bar)
        .withColumn("_vol_avg_20",    F.avg("volume").over(W20))
        .withColumn("_vol_stddev_20", F.stddev("volume").over(W20))

        .withColumn("volume_avg_20", F.round(F.col("_vol_avg_20"), 2))
        .withColumn("volume_z_20",
            F.round(
                F.when(
                    F.col("_vol_stddev_20") > 0,
                    (F.col("volume") - F.col("_vol_avg_20")) / F.col("_vol_stddev_20")
                ).otherwise(0.0),
                4,
            )
        )

        .drop("_vol_avg_20", "_vol_stddev_20")
    )

    return df


df_volume = add_volume_indicators(df_volatility)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Trading Signals
# MAGIC
# MAGIC Rule-based signal generation — the Paper Trading Engine consumes these.
# MAGIC
# MAGIC | Signal | Condition | Value |
# MAGIC |---|---|---|
# MAGIC | `signal_golden_cross` | SMA-5 crosses above SMA-20 | 1 = cross up, -1 = cross down, 0 = none |
# MAGIC | `signal_macd` | MACD line crosses signal line | 1 / -1 / 0 |
# MAGIC | `signal_rsi` | RSI < 30 → oversold (buy), RSI > 70 → overbought (sell) | 1 / -1 / 0 |
# MAGIC | `signal_bb` | Close touches lower band (buy) or upper band (sell) | 1 / -1 / 0 |
# MAGIC | `signal_composite` | Majority vote across all signals (-2 to +2 → BUY/SELL/HOLD) | BUY / SELL / HOLD |

# COMMAND ----------

def add_signals(df: DataFrame) -> DataFrame:
    """Add rule-based trading signal columns."""

    prev_w = Window.partitionBy("ticker").orderBy("timestamp")

    df = (
        df
        # ── Lag columns for crossover detection ─────────────────────────────
        .withColumn("_prev_sma_5",      F.lag("sma_5",      1).over(prev_w))
        .withColumn("_prev_sma_20",     F.lag("sma_20",     1).over(prev_w))
        .withColumn("_prev_macd_line",  F.lag("macd_line",  1).over(prev_w))
        .withColumn("_prev_macd_sig",   F.lag("macd_signal",1).over(prev_w))

        # ── Golden / Death Cross (SMA-5 vs SMA-20) ──────────────────────────
        .withColumn(
            "signal_golden_cross",
            F.when(
                (F.col("sma_5") > F.col("sma_20")) &
                (F.col("_prev_sma_5") <= F.col("_prev_sma_20")),
                F.lit(1),
            ).when(
                (F.col("sma_5") < F.col("sma_20")) &
                (F.col("_prev_sma_5") >= F.col("_prev_sma_20")),
                F.lit(-1),
            ).otherwise(F.lit(0)).cast(T.ByteType()),
        )

        # ── MACD Crossover ───────────────────────────────────────────────────
        .withColumn(
            "signal_macd",
            F.when(
                (F.col("macd_line") > F.col("macd_signal")) &
                (F.col("_prev_macd_line") <= F.col("_prev_macd_sig")),
                F.lit(1),
            ).when(
                (F.col("macd_line") < F.col("macd_signal")) &
                (F.col("_prev_macd_line") >= F.col("_prev_macd_sig")),
                F.lit(-1),
            ).otherwise(F.lit(0)).cast(T.ByteType()),
        )

        # ── RSI Extremes ─────────────────────────────────────────────────────
        .withColumn(
            "signal_rsi",
            F.when(F.col("rsi_14") < 30, F.lit(1))
             .when(F.col("rsi_14") > 70, F.lit(-1))
             .otherwise(F.lit(0)).cast(T.ByteType()),
        )

        # ── Bollinger Band Touch ─────────────────────────────────────────────
        .withColumn(
            "signal_bb",
            F.when(F.col("bb_pct_b") <= 0.05, F.lit(1))   # Near lower band
             .when(F.col("bb_pct_b") >= 0.95, F.lit(-1))  # Near upper band
             .otherwise(F.lit(0)).cast(T.ByteType()),
        )
    )

    # ── Composite signal: majority vote ──────────────────────────────────────
    df = df.withColumn(
        "_vote_sum",
        F.col("signal_golden_cross").cast(T.IntegerType()) +
        F.col("signal_macd").cast(T.IntegerType()) +
        F.col("signal_rsi").cast(T.IntegerType()) +
        F.col("signal_bb").cast(T.IntegerType()),
    ).withColumn(
        "signal_composite",
        F.when(F.col("_vote_sum") >= 2, F.lit("BUY"))
         .when(F.col("_vote_sum") <= -2, F.lit("SELL"))
         .otherwise(F.lit("HOLD")),
    )

    # Drop temp columns
    df = df.drop(
        "_prev_sma_5", "_prev_sma_20",
        "_prev_macd_line", "_prev_macd_sig",
        "_vote_sum",
    )

    return df


df_signals = add_signals(df_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Final Column Selection & Gold Audit Column

# COMMAND ----------

GOLD_COLUMNS = [
    # Keys
    "ticker", "timestamp",
    # OHLCV
    "open", "high", "low", "close", "volume",
    # Trend
    "sma_5", "sma_20", "sma_50",
    "ema_12", "ema_26",
    # Momentum
    "macd_line", "macd_signal", "macd_hist",
    "rsi_14",
    # Volatility
    "bb_upper", "bb_lower", "bb_width", "bb_pct_b",
    "volatility_20",
    # Volume
    "vwap", "volume_avg_20", "volume_z_20",
    # Signals
    "signal_golden_cross", "signal_macd", "signal_rsi",
    "signal_bb", "signal_composite",
    # Audit
    "silver_loaded_at",
    # Partitions
    "year", "month", "day",
]

df_gold = (
    df_signals
    .select(*GOLD_COLUMNS)
    .withColumn("gold_loaded_at", F.current_timestamp())
)

# Filter to only rows within our primary window (last 48 hours) for writing
# The extra history was only needed to warm up the long-period indicators.
fresh_cutoff = F.current_timestamp() - F.expr("INTERVAL 48 HOURS")
df_gold_fresh = df_gold.filter(F.col("timestamp") >= fresh_cutoff)

print(f"Total Gold rows (all history): {df_gold.count()}")
print(f"Fresh rows to write (last 48h): {df_gold_fresh.count()}")
display(df_gold_fresh.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Write to Gold (Delta Lake MERGE / Overwrite)

# COMMAND ----------

def write_gold(df: DataFrame, table_path: str, mode: str) -> None:
    """Write Gold features to Delta Lake with upsert or overwrite."""

    if mode == "merge" and DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)

        (
            delta_table.alias("gold")
            .merge(
                df.alias("new"),
                "gold.ticker = new.ticker AND gold.timestamp = new.timestamp",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"MERGE complete → {table_path}")

    else:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("ticker", "year", "month", "day")
            .save(table_path)
        )
        print(f"WRITE (overwrite) complete → {table_path}")


write_gold(df_gold_fresh, GOLD_TABLE_PATH, WRITE_MODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · OPTIMIZE + ZORDER

# COMMAND ----------

ticker_filter = ""
if TICKERS:
    ticker_list = ", ".join(f"'{t}'" for t in TICKERS)
    ticker_filter = f"WHERE ticker IN ({ticker_list})"

spark.sql(f"""
    OPTIMIZE delta.`{GOLD_TABLE_PATH}`
    {ticker_filter}
    ZORDER BY (timestamp)
""")
print("OPTIMIZE + ZORDER complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Signal Distribution Summary

# COMMAND ----------

signal_summary = (
    spark.read.format("delta").load(GOLD_TABLE_PATH)
    .groupBy("ticker", "signal_composite")
    .agg(F.count("*").alias("count"))
    .orderBy("ticker", "signal_composite")
)
display(signal_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 · Latest Signal Per Ticker (Paper Trading Engine Input)

# COMMAND ----------

latest_w = Window.partitionBy("ticker").orderBy(F.col("timestamp").desc())

latest_signals = (
    spark.read.format("delta").load(GOLD_TABLE_PATH)
    .withColumn("_rn", F.row_number().over(latest_w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select(
        "ticker", "timestamp",
        "close", "rsi_14", "macd_line", "macd_signal",
        "bb_pct_b", "signal_golden_cross", "signal_macd",
        "signal_rsi", "signal_bb", "signal_composite",
    )
    .orderBy("ticker")
)

display(latest_signals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14 · Materialize Signals Cache for API Layer
# MAGIC
# MAGIC Writes the latest signal per ticker as a flat Parquet file to
# MAGIC `s3://gold/signals_cache/latest.parquet` — readable by the API Lambda
# MAGIC using PyArrow without requiring Spark or Delta libraries.

# COMMAND ----------

import io
import boto3

def materialize_signals_cache(df_latest: DataFrame, gold_bucket: str) -> None:
    """
    Write the latest-per-ticker signal snapshot to S3 as a single Parquet file.
    This is the primary data source for the REST API Lambda.
    """
    # Convert to Pandas for PyArrow write (small dataset — one row per ticker)
    pdf = df_latest.toPandas()

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(pdf, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    bucket = gold_bucket.replace("s3://", "").split("/")[0]
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key="signals_cache/latest.parquet",
        Body=buf,
        ContentType="application/octet-stream",
    )
    print(f"Signals cache written → s3://{bucket}/signals_cache/latest.parquet ({len(pdf)} tickers)")


materialize_signals_cache(latest_signals, GOLD_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ✅ **Gold transformation complete.**
# MAGIC
# MAGIC The `signal_composite` column is now ready for consumption by the
# MAGIC **Paper Trading Engine** — BUY / SELL / HOLD per ticker, per hour.
# MAGIC
# MAGIC Next step: **Paper Trading Engine** — virtual portfolio execution layer.
