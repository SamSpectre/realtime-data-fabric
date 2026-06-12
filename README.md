# Real-Time Data Fabric with AI Agents

A streaming lakehouse on Azure Databricks that ingests 1.8M+ GitHub events, processes them through a medallion architecture with exactly-once guarantees, trains and registers an ML model with MLflow, and exposes the resulting Gold tables to a LangGraph agent — with the full pipeline (Ingest → Bronze → Silver → Gold → Score) orchestrated end-to-end in **21.5 seconds**.

## Architecture

```
GitHub Archive ──► Auto Loader (cloudFiles) ──► Bronze (raw JSON)
                        │                          │
              schema rescue + checkpointed         ▼
              exactly-once processing           Silver (parsed, typed, partitioned)
                                                   │
                                                   ▼
                                                Gold (4 analytics tables)
                                                   │
                            ┌──────────────────────┼─────────────────────┐
                            ▼                      ▼                     ▼
                     Feature engineering     Unity Catalog         LangGraph ReAct agent
                     (16 features)           (governance)          (8 SQL tools, GPT-4o)
                            │
                            ▼
                     GradientBoosting → MLflow Experiment + Model Registry
```

## Scale and results

- **1.8M+ GitHub events** processed across 174K repositories and 118K developers
- **21.5s** end-to-end `run_pipeline()`: ingest → Bronze → Silver → Gold → model scoring
- **Exactly-once streaming** via Auto Loader checkpoints with schema-rescue mode for malformed events
- **16-feature model** tracked and versioned in the MLflow Model Registry

## What's here

| Path | Contents |
|---|---|
| `notebooks/01_github_streaming_bronze.py` | The complete pipeline as a Databricks notebook: streaming ingestion, medallion transforms, feature engineering, MLflow training, and the LangGraph agent |
| `scripts/01_verify_connection.py` | Databricks workspace connection check |
| `scripts/02_setup_catalog.py` | Unity Catalog schema/volume setup |
| `scripts/04_setup_secrets.py` | Databricks Secrets for API keys (no keys in code) |

## Key engineering decisions

- **Auto Loader over plain readStream** — incremental ingestion with schema evolution and a rescue column, so malformed events land in Bronze instead of killing the stream.
- **`trigger(availableNow=True)`** — batch-style execution of streaming logic: identical code path for backfill and continuous operation.
- **Unity Catalog from day one** — tables, volumes, and secrets governed centrally rather than retrofitted.
- **Agent reads Gold only** — the LangGraph agent's 8 SQL tools target curated Gold tables, never raw data, keeping cost and blast radius bounded.

## Running it

Requires an Azure Databricks workspace (Unity Catalog enabled) and a GitHub Archive sample in cloud storage.

```bash
pip install -r requirements.txt
cp .env.example .env                    # Databricks host + token
python scripts/01_verify_connection.py
python scripts/02_setup_catalog.py
python scripts/04_setup_secrets.py
# then import notebooks/01_github_streaming_bronze.py into your workspace and run
```

## Stack

Azure Databricks · Delta Lake · Unity Catalog · PySpark Structured Streaming · Auto Loader · MLflow · scikit-learn · LangGraph · GPT-4o

## License

MIT
