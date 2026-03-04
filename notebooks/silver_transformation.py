# Databricks notebook source
# Silver Layer Transformation — Shadow Trader
# fmt: off
# This file uses the Databricks source format (# COMMAND ---------- separators).
# Import into Databricks via: Workspace → Import → select this .py file.

# MAGIC %md
# MAGIC # 🥈 Shadow Trader — Silver Layer Transformation
# MAGIC
# MAGIC **Layer**: Bronze → Silver
# MAGIC
# MAGIC **Purpose**: Reads raw Parquet files from the Bronze S3 bucket, applies
# MAGIC an explicit schema, deduplicates, runs data-quality checks, and writes
# MAGIC clean **Delta Lake** tables to the Silver bucket with Hive-style partitioning.
# MAGIC
# MAGIC | Step | Action |
# MAGIC |---|---|
# MAGIC | 1 | Read partitioned Parquet from Bronze |
# MAGIC | 2 | Enforce explicit Spark schema |
# MAGIC | 3 | Rename/cast columns |
# MAGIC | 4 | Filter last N hours (configurable) |
# MAGIC | 5 | Deduplicate on (ticker, timestamp) |
# MAGIC | 6 | Data-quality assertions |
# MAGIC | 7 | MERGE into Silver Delta table (upsert) |
# MAGIC | 8 | OPTIMIZE + ZORDER for query performance |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Widget Parameters
# MAGIC
# MAGIC Run this cell first — Databricks will render these as UI dropdowns/inputs
# MAGIC at the top of the notebook.

# COMMAND ----------

dbutils.widgets.text(
    "bronze_path",
    "s3://shadow-trader-bronze-dev",
    "Bronze S3 Path",
)
dbutils.widgets.text(
    "silver_path",
    "s3://shadow-trader-silver-dev",
    "Silver S3 Path (Delta root)",
)
dbutils.widgets.text(
    "tickers",
    "BTC,NVDA,ETH",
    "Tickers (comma-separated, empty = all)",
)
dbutils.widgets.text(
    "lookback_hours",
    "48",
    "Lookback Window (hours)",
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_transformation")

# ── Read widget values ────────────────────────────────────────────────────────
BRONZE_PATH     = dbutils.widgets.get("bronze_path").rstrip("/")
SILVER_PATH     = dbutils.widgets.get("silver_path").rstrip("/")
TICKERS_RAW     = dbutils.widgets.get("tickers")
LOOKBACK_HOURS  = int(dbutils.widgets.get("lookback_hours"))
WRITE_MODE      = dbutils.widgets.get("write_mode")

TICKERS = [t.strip().upper() for t in TICKERS_RAW.split(",") if t.strip()] or None

# Silver Delta table path
SILVER_TABLE_PATH = f"{SILVER_PATH}/market_data_silver"

print(f"Bronze  : {BRONZE_PATH}")
print(f"Silver  : {SILVER_TABLE_PATH}")
print(f"Tickers : {TICKERS or 'ALL'}")
print(f"Lookback: {LOOKBACK_HOURS}h")
print(f"Mode    : {WRITE_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Bronze Schema Definition
# MAGIC
# MAGIC Declaring the expected schema avoids Spark reading every Parquet footer
# MAGIC during planning (schema inference) and makes type errors explicit.

# COMMAND ----------

BRONZE_SCHEMA = T.StructType([
    T.StructField("timestamp",    T.TimestampType(), nullable=False),
    T.StructField("open",         T.DoubleType(),    nullable=True),
    T.StructField("high",         T.DoubleType(),    nullable=True),
    T.StructField("low",          T.DoubleType(),    nullable=True),
    T.StructField("close",        T.DoubleType(),    nullable=True),
    T.StructField("volume",       T.DoubleType(),    nullable=True),
    T.StructField("ticker",       T.StringType(),    nullable=False),
    T.StructField("processed_at", T.StringType(),    nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Read Bronze Parquet

# COMMAND ----------

def read_bronze(spark: SparkSession, base_path: str, tickers: list | None, lookback_hours: int) -> DataFrame:
    """
    Read Hive-partitioned Parquet from the Bronze layer.
    Applies partition pruning via path glob when tickers are specified.
    """
    # Calculate date range for partition pruning
    now = datetime.now(tz=timezone.utc)
    start_dt = now - timedelta(hours=lookback_hours)

    # Build glob paths to leverage S3 partition pruning
    if tickers:
        paths = [f"{base_path}/ticker={t}/*/*/*/data.parquet" for t in tickers]
    else:
        paths = [f"{base_path}/ticker=*/*/*/*/data.parquet"]

    logger.info("Reading Bronze paths: %s", paths)

    df = (
        spark.read
        .schema(BRONZE_SCHEMA)
        .option("mergeSchema", "false")   # Strict — reject schema drift
        .option("basePath", base_path)    # Preserve partition columns in df
        .parquet(*paths)
    )

    # Apply time filter (partition-level pruning already narrows files)
    df = df.filter(F.col("timestamp") >= F.lit(start_dt))

    logger.info("Bronze row count (pre-dedup): %d", df.count())
    return df


df_bronze = read_bronze(spark, BRONZE_PATH, TICKERS, LOOKBACK_HOURS)
display(df_bronze.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Transform & Enrich

# COMMAND ----------

def transform(df: DataFrame) -> DataFrame:
    """
    Apply Silver-layer transformations:
    - Derive date partition columns from timestamp
    - Calculate bar_range (high - low) and mid_price
    - Cast processed_at to timestamp
    - Add silver_loaded_at audit column
    """
    return (
        df
        # ── Partition columns ───────────────────────────────────────────────
        .withColumn("year",  F.year("timestamp").cast(T.ShortType()))
        .withColumn("month", F.month("timestamp").cast(T.ByteType()))
        .withColumn("day",   F.dayofmonth("timestamp").cast(T.ByteType()))

        # ── Derived metrics ─────────────────────────────────────────────────
        .withColumn("bar_range",  F.round(F.col("high") - F.col("low"), 8))
        .withColumn("mid_price",  F.round((F.col("high") + F.col("low")) / 2, 8))
        .withColumn("close_pct_change",
            F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4)
        )

        # ── Audit ───────────────────────────────────────────────────────────
        .withColumn("processed_at",    F.to_timestamp("processed_at"))
        .withColumn("silver_loaded_at", F.current_timestamp())

        # ── Canonical column order ──────────────────────────────────────────
        .select(
            "ticker",
            "timestamp",
            "open", "high", "low", "close",
            "volume",
            "bar_range",
            "mid_price",
            "close_pct_change",
            "processed_at",
            "silver_loaded_at",
            "year", "month", "day",
        )
    )


df_transformed = transform(df_bronze)
display(df_transformed.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Deduplication
# MAGIC
# MAGIC Deduplicate on the natural key **(ticker, timestamp)** keeping the record
# MAGIC with the latest `processed_at` — handles Lambda re-runs gracefully.

# COMMAND ----------

from pyspark.sql.window import Window

dedup_window = Window.partitionBy("ticker", "timestamp").orderBy(F.col("processed_at").desc())

df_deduped = (
    df_transformed
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

dup_count = df_transformed.count() - df_deduped.count()
print(f"Duplicates removed : {dup_count}")
print(f"Clean row count    : {df_deduped.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Data Quality Checks
# MAGIC
# MAGIC Assertions that must pass before writing to Silver.
# MAGIC A failure here raises an exception, stopping the pipeline cleanly.

# COMMAND ----------

def run_dq_checks(df: DataFrame) -> None:
    """Run data-quality assertions. Raises ValueError on failure."""
    errors = []

    total = df.count()
    if total == 0:
        raise ValueError("DQ FAIL: DataFrame is empty after transformation.")

    # Null checks on critical columns
    critical_cols = ["ticker", "timestamp", "open", "high", "low", "close"]
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in critical_cols]
    ).collect()[0].asDict()

    for col_name, null_ct in null_counts.items():
        if null_ct > 0:
            errors.append(f"  ✗ {col_name}: {null_ct} null values ({null_ct/total:.1%})")

    # Sanity: high >= low
    invalid_hl = df.filter(F.col("high") < F.col("low")).count()
    if invalid_hl > 0:
        errors.append(f"  ✗ {invalid_hl} rows where high < low")

    # Sanity: prices must be positive
    neg_prices = df.filter(
        (F.col("open") <= 0) | (F.col("close") <= 0)
    ).count()
    if neg_prices > 0:
        errors.append(f"  ✗ {neg_prices} rows with non-positive open/close prices")

    # Volume must be non-negative
    neg_vol = df.filter(F.col("volume") < 0).count()
    if neg_vol > 0:
        errors.append(f"  ✗ {neg_vol} rows with negative volume")

    if errors:
        raise ValueError("Data Quality Checks FAILED:\n" + "\n".join(errors))

    print(f"✓ All DQ checks passed on {total:,} rows.")


run_dq_checks(df_deduped)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Write to Silver (Delta Lake MERGE / Overwrite)

# COMMAND ----------

def write_silver(df: DataFrame, table_path: str, mode: str) -> None:
    """
    Write the clean DataFrame to the Silver Delta table.

    - mode='merge'     → Upsert on (ticker, timestamp); idempotent re-runs.
    - mode='overwrite' → Full overwrite; useful for backfills.
    """
    if mode == "merge" and DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)

        (
            delta_table.alias("silver")
            .merge(
                df.alias("bronze"),
                "silver.ticker = bronze.ticker AND silver.timestamp = bronze.timestamp",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"MERGE complete → {table_path}")

    else:
        # First run or explicit overwrite
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("ticker", "year", "month", "day")
            .save(table_path)
        )
        print(f"WRITE (overwrite) complete → {table_path}")


write_silver(df_deduped, SILVER_TABLE_PATH, WRITE_MODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · OPTIMIZE + ZORDER
# MAGIC
# MAGIC Compacts small files and co-locates data by `timestamp` within each
# MAGIC ticker partition — dramatically speeds up time-range queries from the
# MAGIC Gold layer and Databricks SQL.

# COMMAND ----------

ticker_filter = ""
if TICKERS:
    ticker_list = ", ".join(f"'{t}'" for t in TICKERS)
    ticker_filter = f"WHERE ticker IN ({ticker_list})"

optimize_sql = f"""
    OPTIMIZE delta.`{SILVER_TABLE_PATH}`
    {ticker_filter}
    ZORDER BY (timestamp)
"""

print(f"Running: {optimize_sql.strip()}")
spark.sql(optimize_sql)
print("OPTIMIZE + ZORDER complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Row Count Summary & Verification

# COMMAND ----------

summary = (
    spark.read.format("delta").load(SILVER_TABLE_PATH)
    .groupBy("ticker")
    .agg(
        F.count("*").alias("total_rows"),
        F.min("timestamp").alias("earliest"),
        F.max("timestamp").alias("latest"),
        F.round(F.avg("close"), 4).alias("avg_close"),
        F.round(F.avg("volume"), 2).alias("avg_volume"),
    )
    .orderBy("ticker")
)

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Delta Table History (Audit)

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_TABLE_PATH}`").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ✅ **Silver transformation complete.**
# MAGIC Next step: **Gold Layer** — rolling averages, volatility metrics, and trading signals.
