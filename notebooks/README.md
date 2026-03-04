# Notebooks — Shadow Trader

## Files

| Notebook | Layer | Description |
|---|---|---|
| [`silver_transformation.py`](./silver_transformation.py) | Silver | Bronze → Silver Delta MERGE |
| [`gold_features.py`](./gold_features.py) | Gold | Silver → Gold: indicators + trading signals |

---

## How to Import into Databricks

1. In your Databricks workspace, go to **Workspace → your folder → ⋮ → Import**
2. Select **Source file (.py)**
3. Upload `silver_transformation.py`
4. Databricks converts each `# COMMAND ----------` block into a notebook cell

> The `# MAGIC %md` prefix renders Markdown cells inside Databricks automatically.

---

## Cluster Requirements

| Requirement | Recommended Value |
|---|---|
| Databricks Runtime | **DBR 14.3 LTS** or later (Delta 3.x, Spark 3.5) |
| Node type | `i3.xlarge` (memory-optimised, local SSD) |
| Autoscaling | Min 1 / Max 4 workers |
| Spot instances | Enabled (driver = on-demand) |

**Cluster libraries** — attach via UI or `cluster_libraries.json`:
```
delta-spark  (included in DBR 14+ automatically)
```
No extra pip installs needed — PySpark, Delta, and dbutils are built-in.

---

## AWS Permissions for the Cluster

The Databricks cluster role (Instance Profile) needs read access to Bronze
and write access to Silver:

```json
{
  "Action": ["s3:GetObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::shadow-trader-bronze-dev/*",
    "arn:aws:s3:::shadow-trader-bronze-dev"
  ]
},
{
  "Action": ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetObject"],
  "Resource": [
    "arn:aws:s3:::shadow-trader-silver-dev/*",
    "arn:aws:s3:::shadow-trader-silver-dev"
  ]
}
```

---

## Widget Parameters (top of notebook)

| Widget | Default | Description |
|---|---|---|
| `bronze_path` | `s3://shadow-trader-bronze-dev` | Source Bronze S3 path |
| `silver_path` | `s3://shadow-trader-silver-dev` | Target Silver Delta root |
| `tickers` | `BTC,NVDA,ETH` | Comma-separated (empty = all) |
| `lookback_hours` | `48` | How far back to read |
| `write_mode` | `merge` | `merge` (upsert) or `overwrite` |

---

## Scheduling

Run this notebook on a schedule via **Databricks Workflows**:

1. Workflows → **Create Job**
2. Task type: **Notebook**
3. Source: select `silver_transformation.py`
4. Schedule: **Hourly**, offset by 10 min after Lambda (e.g. `10 * * * *`)
5. Add cluster config from above

The 10-minute offset gives the Lambda time to finish writing Bronze before Silver starts reading.
