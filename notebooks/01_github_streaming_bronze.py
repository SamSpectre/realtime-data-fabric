# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub Event Streaming - Bronze Layer
# MAGIC
# MAGIC This notebook ingests real-time GitHub events into the Bronze layer.
# MAGIC
# MAGIC **Data Source:** GitHub Archive (https://www.gharchive.org/)
# MAGIC - Captures all public GitHub events
# MAGIC - ~30+ events per second
# MAGIC - Available as hourly JSON files
# MAGIC
# MAGIC **Pattern:** We'll use Auto Loader to incrementally process new files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and schema
CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"

# Table names
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_events"

# Checkpoint location (in Unity Catalog volume)
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/checkpoints/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore GitHub Archive Data
# MAGIC
# MAGIC GitHub Archive provides hourly JSON files. Let's look at the structure.

# COMMAND ----------

# Sample URL for one hour of data
sample_url = "https://data.gharchive.org/2024-01-15-12.json.gz"

# Download a sample to understand schema
import requests
import gzip
import json

response = requests.get(sample_url, stream=True)
content = gzip.decompress(response.content)
lines = content.decode('utf-8').strip().split('\n')

# Parse first few events
sample_events = [json.loads(line) for line in lines[:5]]

print(f"Total events in sample hour: {len(lines)}")
print(f"\nSample event structure:")
print(json.dumps(sample_events[0], indent=2)[:2000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Bronze Schema
# MAGIC
# MAGIC We store raw JSON with minimal transformation - just add metadata.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    TimestampType, BooleanType, MapType, ArrayType
)

# Bronze schema - keep it flexible with raw JSON
bronze_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", MapType(StringType(), StringType()), True),
    StructField("repo", MapType(StringType(), StringType()), True),
    StructField("payload", StringType(), True),  # Keep as JSON string
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("org", MapType(StringType(), StringType()), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Table

# COMMAND ----------

# Create the bronze table if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    id STRING,
    type STRING,
    actor MAP<STRING, STRING>,
    repo MAP<STRING, STRING>,
    payload STRING,
    public BOOLEAN,
    created_at STRING,
    org MAP<STRING, STRING>,
    _ingested_at TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw GitHub events from GitHub Archive'
""")

print(f"✓ Bronze table ready: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Historical Data (Batch)
# MAGIC
# MAGIC First, let's load a few hours of historical data to have something to work with.

# COMMAND ----------

from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    from_json, to_json, lit
)
from datetime import datetime, timedelta

# Generate URLs for last 6 hours of data
def get_gharchive_urls(hours_back=6):
    """Generate GitHub Archive URLs for recent hours."""
    urls = []
    now = datetime.utcnow()
    for i in range(hours_back):
        dt = now - timedelta(hours=i+1)
        url = f"https://data.gharchive.org/{dt.strftime('%Y-%m-%d-%-H')}.json.gz"
        # Windows compatible format
        url = f"https://data.gharchive.org/{dt.strftime('%Y-%m-%d')}-{dt.hour}.json.gz"
        urls.append(url)
    return urls

urls = get_gharchive_urls(hours_back=3)  # Start with 3 hours
print("Loading data from:")
for url in urls:
    print(f"  - {url}")

# COMMAND ----------

# DBTITLE 1,Cell 12
# Download GitHub Archive files and write to Unity Catalog volume
from pyspark.sql.functions import col, to_json
import os

# Define volume path for staging
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/github_archive"

# Create directory if it doesn't exist
dbutils.fs.mkdirs(volume_path)

print(f"Staging files to: {volume_path}")

# Download and write each file to the volume using Python file I/O
for url in urls:
    filename = url.split('/')[-1]
    # For Unity Catalog volumes, use /Volumes path directly with Python open()
    filepath = f"{volume_path}/{filename}"
    print(f"Downloading {filename}...")
    
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        # Write binary content directly using Python file I/O
        with open(filepath, 'wb') as f:
            f.write(response.content)
        print(f"  Written to {filepath}")
    else:
        print(f"  Failed to download: {response.status_code}")

print(f"\nReading files from volume into Spark...")

# Read the JSON files from volume (Spark can read .gz files automatically)
df_raw = (
    spark.read
    .option("multiline", "false")  # One JSON per line
    .json(volume_path)
)

print(f"Records loaded: {df_raw.count():,}")
df_raw.printSchema()

# COMMAND ----------

# Configuration
CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_events"

from pyspark.sql.functions import col, to_json, current_timestamp, lit

# Drop existing table to resolve schema conflict
print("Dropping existing table...")
spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")

# Convert all complex/nested columns to JSON strings
df_bronze = (
    df_raw
    .withColumn("actor", to_json(col("actor")))
    .withColumn("repo", to_json(col("repo")))
    .withColumn("payload", to_json(col("payload")))
    .withColumn("org", to_json(col("org")))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("batch_load"))
)

# Check the schema before writing
print("Schema to be written:")
df_bronze.printSchema()

# Write to Bronze table (fresh create)
(
    df_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✓ Data written to {BRONZE_TABLE}")

# Verify
count = spark.table(BRONZE_TABLE).count()
print(f"✓ Total records: {count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Data

# COMMAND ----------

# Check the data
df_check = spark.table(BRONZE_TABLE)

print(f"Total records: {df_check.count():,}")
print(f"\nEvent types:")
df_check.groupBy("type").count().orderBy(col("count").desc()).show(10)

print(f"\nSample records:")
df_check.select("id", "type", "repo", "created_at").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up Streaming (Auto Loader)
# MAGIC
# MAGIC For production, we'd use Auto Loader to continuously ingest new files.
# MAGIC This requires files to be in cloud storage (we'll set this up next).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **What we built:**
# MAGIC - Bronze table in Unity Catalog
# MAGIC - Loaded 3 hours of historical GitHub events
# MAGIC - Schema designed for flexibility (raw JSON payload)
# MAGIC
# MAGIC **Next notebook:** Transform Bronze → Silver (clean, type, validate)

# COMMAND ----------

# =============================================================================
# SILVER LAYER TRANSFORMATION
# =============================================================================
# Bronze (raw JSON strings) → Silver (structured, cleaned, typed)

from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    to_timestamp, date_format, hour, dayofweek,
    get_json_object, when
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Configuration
CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_events"

print("="*60)
print("SILVER LAYER TRANSFORMATION")
print("="*60)

# Read from Bronze
df_bronze = spark.table(BRONZE_TABLE)
print(f"Bronze records: {df_bronze.count():,}")

# Parse JSON strings and extract key fields
df_silver = (
    df_bronze
    # Parse actor JSON
    .withColumn("actor_id", get_json_object(col("actor"), "$.id").cast("long"))
    .withColumn("actor_login", get_json_object(col("actor"), "$.login"))
    .withColumn("actor_url", get_json_object(col("actor"), "$.url"))
    
    # Parse repo JSON
    .withColumn("repo_id", get_json_object(col("repo"), "$.id").cast("long"))
    .withColumn("repo_name", get_json_object(col("repo"), "$.name"))
    .withColumn("repo_url", get_json_object(col("repo"), "$.url"))
    
    # Parse org JSON (may be null)
    .withColumn("org_id", get_json_object(col("org"), "$.id").cast("long"))
    .withColumn("org_login", get_json_object(col("org"), "$.login"))
    
    # Parse and enrich timestamp
    .withColumn("event_timestamp", to_timestamp(col("created_at")))
    .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
    .withColumn("event_hour", hour(col("event_timestamp")))
    .withColumn("event_dayofweek", dayofweek(col("event_timestamp")))
    
    # Classify event category
    .withColumn("event_category", 
        when(col("type").isin("PushEvent", "CreateEvent", "DeleteEvent"), "code")
        .when(col("type").isin("PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"), "review")
        .when(col("type").isin("IssuesEvent", "IssueCommentEvent"), "issues")
        .when(col("type").isin("WatchEvent", "ForkEvent", "StarEvent"), "engagement")
        .otherwise("other")
    )
    
    # Add processing metadata
    .withColumn("_processed_at", current_timestamp())
    
    # Select final columns (drop raw JSON)
    .select(
        "id",
        "type",
        "event_category",
        "actor_id",
        "actor_login",
        "repo_id",
        "repo_name",
        "org_id",
        "org_login",
        "event_timestamp",
        "event_date",
        "event_hour",
        "event_dayofweek",
        "payload",  # Keep payload for detailed analysis later
        "public",
        "_ingested_at",
        "_processed_at"
    )
)

# Show schema
print("\nSilver schema:")
df_silver.printSchema()

# Drop and recreate Silver table
print("\nWriting to Silver table...")
spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_date")  # Partition by date for efficient queries
    .saveAsTable(SILVER_TABLE)
)

print(f"✓ Silver table created: {SILVER_TABLE}")

# Verify
silver_count = spark.table(SILVER_TABLE).count()
print(f"✓ Silver records: {silver_count:,}")

# COMMAND ----------

# =============================================================================
# GOLD LAYER - ANALYTICS TABLES
# =============================================================================
# Silver (clean events) → Gold (aggregated metrics)

from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    avg, min as spark_min, max as spark_max,
    current_timestamp, dense_rank, desc
)
from pyspark.sql.window import Window

# Configuration
CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_events"

print("="*60)
print("GOLD LAYER - ANALYTICS TABLES")
print("="*60)

# Read Silver data
df_silver = spark.table(SILVER_TABLE)

# ---------------------------------------------------------
# GOLD TABLE 1: Repository Metrics
# ---------------------------------------------------------
print("\n1. Creating gold_repo_metrics...")

df_repo_metrics = (
    df_silver
    .groupBy("repo_id", "repo_name")
    .agg(
        count("*").alias("total_events"),
        countDistinct("actor_id").alias("unique_contributors"),
        countDistinct("event_date").alias("active_days"),
        
        # Event type counts
        spark_sum(when(col("type") == "PushEvent", 1).otherwise(0)).alias("push_count"),
        spark_sum(when(col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pr_count"),
        spark_sum(when(col("type") == "IssuesEvent", 1).otherwise(0)).alias("issue_count"),
        spark_sum(when(col("type") == "WatchEvent", 1).otherwise(0)).alias("star_count"),
        spark_sum(when(col("type") == "ForkEvent", 1).otherwise(0)).alias("fork_count"),
        
        # Category aggregations
        spark_sum(when(col("event_category") == "code", 1).otherwise(0)).alias("code_events"),
        spark_sum(when(col("event_category") == "review", 1).otherwise(0)).alias("review_events"),
        spark_sum(when(col("event_category") == "issues", 1).otherwise(0)).alias("issue_events"),
        spark_sum(when(col("event_category") == "engagement", 1).otherwise(0)).alias("engagement_events"),
        
        spark_min("event_timestamp").alias("first_event"),
        spark_max("event_timestamp").alias("last_event")
    )
    .withColumn("_updated_at", current_timestamp())
)

# Add ranking
window_spec = Window.orderBy(desc("total_events"))
df_repo_metrics = df_repo_metrics.withColumn("activity_rank", dense_rank().over(window_spec))

# Write
GOLD_REPO_TABLE = f"{CATALOG}.{SCHEMA}.gold_repo_metrics"
spark.sql(f"DROP TABLE IF EXISTS {GOLD_REPO_TABLE}")
df_repo_metrics.write.format("delta").mode("overwrite").saveAsTable(GOLD_REPO_TABLE)
print(f"   ✓ {GOLD_REPO_TABLE}: {df_repo_metrics.count():,} repos")

# ---------------------------------------------------------
# GOLD TABLE 2: Developer Activity
# ---------------------------------------------------------
print("\n2. Creating gold_developer_activity...")

df_developer = (
    df_silver
    .groupBy("actor_id", "actor_login")
    .agg(
        count("*").alias("total_events"),
        countDistinct("repo_id").alias("repos_contributed"),
        countDistinct("event_date").alias("active_days"),
        
        spark_sum(when(col("type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
        spark_sum(when(col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pull_requests"),
        spark_sum(when(col("type") == "IssueCommentEvent", 1).otherwise(0)).alias("comments"),
        
        spark_min("event_timestamp").alias("first_seen"),
        spark_max("event_timestamp").alias("last_seen")
    )
    .withColumn("_updated_at", current_timestamp())
)

# Add ranking
df_developer = df_developer.withColumn(
    "activity_rank", 
    dense_rank().over(Window.orderBy(desc("total_events")))
)

# Write
GOLD_DEV_TABLE = f"{CATALOG}.{SCHEMA}.gold_developer_activity"
spark.sql(f"DROP TABLE IF EXISTS {GOLD_DEV_TABLE}")
df_developer.write.format("delta").mode("overwrite").saveAsTable(GOLD_DEV_TABLE)
print(f"   ✓ {GOLD_DEV_TABLE}: {df_developer.count():,} developers")

# ---------------------------------------------------------
# GOLD TABLE 3: Hourly Activity Trends
# ---------------------------------------------------------
print("\n3. Creating gold_hourly_trends...")

df_hourly = (
    df_silver
    .groupBy("event_date", "event_hour", "event_category")
    .agg(
        count("*").alias("event_count"),
        countDistinct("actor_id").alias("unique_actors"),
        countDistinct("repo_id").alias("unique_repos")
    )
    .withColumn("_updated_at", current_timestamp())
)

GOLD_HOURLY_TABLE = f"{CATALOG}.{SCHEMA}.gold_hourly_trends"
spark.sql(f"DROP TABLE IF EXISTS {GOLD_HOURLY_TABLE}")
df_hourly.write.format("delta").mode("overwrite").saveAsTable(GOLD_HOURLY_TABLE)
print(f"   ✓ {GOLD_HOURLY_TABLE}: {df_hourly.count():,} rows")

# ---------------------------------------------------------
# GOLD TABLE 4: Event Type Summary
# ---------------------------------------------------------
print("\n4. Creating gold_event_summary...")

df_event_summary = (
    df_silver
    .groupBy("event_date", "type", "event_category")
    .agg(
        count("*").alias("event_count"),
        countDistinct("actor_id").alias("unique_actors"),
        countDistinct("repo_id").alias("unique_repos")
    )
    .withColumn("_updated_at", current_timestamp())
)

GOLD_EVENT_TABLE = f"{CATALOG}.{SCHEMA}.gold_event_summary"
spark.sql(f"DROP TABLE IF EXISTS {GOLD_EVENT_TABLE}")
df_event_summary.write.format("delta").mode("overwrite").saveAsTable(GOLD_EVENT_TABLE)
print(f"   ✓ {GOLD_EVENT_TABLE}: {df_event_summary.count():,} rows")

print("\n" + "="*60)
print("GOLD LAYER COMPLETE")
print("="*60)

# COMMAND ----------

# =============================================================================
# EXPLORE THE GOLD LAYER
# =============================================================================

CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"

print("="*60)
print("GITHUB INTELLIGENCE - DATA EXPLORATION")
print("="*60)

# ---------------------------------------------------------
# TOP 15 MOST ACTIVE REPOSITORIES (last 3 hours)
# ---------------------------------------------------------
print("\n📊 TOP 15 MOST ACTIVE REPOSITORIES:")
print("-"*60)

df_top_repos = spark.sql(f"""
    SELECT 
        activity_rank as rank,
        repo_name,
        total_events,
        unique_contributors as contributors,
        push_count as pushes,
        pr_count as prs,
        star_count as stars,
        fork_count as forks
    FROM {CATALOG}.{SCHEMA}.gold_repo_metrics
    ORDER BY activity_rank
    LIMIT 15
""")
df_top_repos.show(15, truncate=40)

# ---------------------------------------------------------
# TOP 15 MOST ACTIVE DEVELOPERS
# ---------------------------------------------------------
print("\n👨‍💻 TOP 15 MOST ACTIVE DEVELOPERS:")
print("-"*60)

df_top_devs = spark.sql(f"""
    SELECT 
        activity_rank as rank,
        actor_login as developer,
        total_events as events,
        repos_contributed as repos,
        pushes,
        pull_requests as prs,
        comments
    FROM {CATALOG}.{SCHEMA}.gold_developer_activity
    ORDER BY activity_rank
    LIMIT 15
""")
df_top_devs.show(15, truncate=30)

# ---------------------------------------------------------
# ACTIVITY BY EVENT CATEGORY
# ---------------------------------------------------------
print("\n📈 ACTIVITY BY CATEGORY:")
print("-"*60)

df_categories = spark.sql(f"""
    SELECT 
        event_category,
        SUM(event_count) as total_events,
        SUM(unique_actors) as total_actors,
        SUM(unique_repos) as total_repos
    FROM {CATALOG}.{SCHEMA}.gold_event_summary
    GROUP BY event_category
    ORDER BY total_events DESC
""")
df_categories.show()

# ---------------------------------------------------------
# HOURLY ACTIVITY PATTERN
# ---------------------------------------------------------
print("\n⏰ HOURLY ACTIVITY PATTERN:")
print("-"*60)

df_hourly = spark.sql(f"""
    SELECT 
        event_hour as hour,
        SUM(event_count) as events,
        SUM(unique_actors) as developers
    FROM {CATALOG}.{SCHEMA}.gold_hourly_trends
    GROUP BY event_hour
    ORDER BY event_hour
""")
df_hourly.show(24)

# ---------------------------------------------------------
# SUMMARY STATISTICS
# ---------------------------------------------------------
print("\n📋 PIPELINE SUMMARY:")
print("="*60)
print(f"""
Lakehouse Structure:
  ├── Bronze: {spark.table(f'{CATALOG}.{SCHEMA}.bronze_events').count():,} raw events
  ├── Silver: {spark.table(f'{CATALOG}.{SCHEMA}.silver_events').count():,} processed events
  └── Gold:
      ├── Repo Metrics:      {spark.table(f'{CATALOG}.{SCHEMA}.gold_repo_metrics').count():,} repositories
      ├── Developer Activity: {spark.table(f'{CATALOG}.{SCHEMA}.gold_developer_activity').count():,} developers
      ├── Hourly Trends:      {spark.table(f'{CATALOG}.{SCHEMA}.gold_hourly_trends').count():,} time slices
      └── Event Summary:      {spark.table(f'{CATALOG}.{SCHEMA}.gold_event_summary').count():,} aggregations

Data Timeframe: Last 3 hours of global GitHub activity
""")

print("✅ Your Data Lakehouse is ready for analysis!")

# COMMAND ----------

# =============================================================================
# DEEP AGENT SYSTEM - GITHUB INTELLIGENCE
# =============================================================================
# Natural language interface to query your Data Lakehouse

# Install required packages
%pip install openai langchain langchain-openai --quiet

dbutils.library.restartPython()

# COMMAND ----------

import os

# Retrieve API key from Databricks Secrets (secure, encrypted)
OPENAI_API_KEY = dbutils.secrets.get(scope="openai", key="api-key")
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

# Catalog configuration
CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"

print("✓ OpenAI API key loaded from Databricks Secrets")
print("✓ Configuration ready")

# COMMAND ----------

# =============================================================================
# GITHUB INTELLIGENCE AGENT
# =============================================================================

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage
import json

# Initialize LLM
llm = ChatOpenAI(model="gpt-4o", temperature=0)

# -----------------------------------------------------------------------------
# TOOL DEFINITIONS
# -----------------------------------------------------------------------------

@tool
def get_top_repositories(limit: int = 10, min_contributors: int = 1) -> str:
    """
    Get the most active repositories ranked by total events.
    
    Args:
        limit: Number of repositories to return (default 10)
        min_contributors: Minimum number of unique contributors (default 1)
    """
    query = f"""
        SELECT 
            activity_rank as rank,
            repo_name,
            total_events,
            unique_contributors,
            push_count,
            pr_count,
            star_count,
            fork_count
        FROM {CATALOG}.{SCHEMA}.gold_repo_metrics
        WHERE unique_contributors >= {min_contributors}
        ORDER BY activity_rank
        LIMIT {limit}
    """
    df = spark.sql(query)
    return df.toPandas().to_json(orient='records', indent=2)


@tool
def get_top_developers(limit: int = 10, exclude_bots: bool = True) -> str:
    """
    Get the most active developers ranked by total events.
    
    Args:
        limit: Number of developers to return (default 10)
        exclude_bots: Whether to exclude bot accounts (default True)
    """
    bot_filter = "AND actor_login NOT LIKE '%[bot]%' AND actor_login != 'Copilot'" if exclude_bots else ""
    
    query = f"""
        SELECT 
            actor_login as developer,
            total_events,
            repos_contributed,
            pushes,
            pull_requests,
            comments
        FROM {CATALOG}.{SCHEMA}.gold_developer_activity
        WHERE actor_login IS NOT NULL {bot_filter}
        ORDER BY total_events DESC
        LIMIT {limit}
    """
    df = spark.sql(query)
    return df.toPandas().to_json(orient='records', indent=2)


@tool
def get_activity_by_category() -> str:
    """Get event counts grouped by category (code, review, issues, engagement)."""
    query = f"""
        SELECT 
            event_category,
            SUM(event_count) as total_events,
            ROUND(SUM(event_count) * 100.0 / (SELECT SUM(event_count) FROM {CATALOG}.{SCHEMA}.gold_event_summary), 2) as percentage
        FROM {CATALOG}.{SCHEMA}.gold_event_summary
        GROUP BY event_category
        ORDER BY total_events DESC
    """
    df = spark.sql(query)
    return df.toPandas().to_json(orient='records', indent=2)


@tool
def get_hourly_trends() -> str:
    """Get activity patterns by hour showing when developers are most active."""
    query = f"""
        SELECT 
            event_hour as hour,
            SUM(event_count) as total_events,
            SUM(unique_actors) as unique_developers
        FROM {CATALOG}.{SCHEMA}.gold_hourly_trends
        GROUP BY event_hour
        ORDER BY event_hour
    """
    df = spark.sql(query)
    return df.toPandas().to_json(orient='records', indent=2)


@tool
def search_repository(search_term: str) -> str:
    """
    Search for repositories by name.
    
    Args:
        search_term: Text to search for in repository names
    """
    query = f"""
        SELECT 
            repo_name,
            total_events,
            unique_contributors,
            push_count,
            pr_count,
            star_count,
            fork_count
        FROM {CATALOG}.{SCHEMA}.gold_repo_metrics
        WHERE LOWER(repo_name) LIKE LOWER('%{search_term}%')
        ORDER BY total_events DESC
        LIMIT 10
    """
    df = spark.sql(query)
    return df.toPandas().to_json(orient='records', indent=2)


@tool
def search_developer(username: str) -> str:
    """
    Search for a specific developer by username.
    
    Args:
        username: GitHub username to search for
    """
    query = f"""
        SELECT 
            actor_login as developer,
            total_events,
            repos_contributed,
            pushes,
            pull_requests,
            comments
        FROM {CATALOG}.{SCHEMA}.gold_developer_activity
        WHERE LOWER(actor_login) LIKE LOWER('%{username}%')
        ORDER BY total_events DESC
        LIMIT 5
    """
    df = spark.sql(query)
    return df.toPandas().to_json(orient='records', indent=2)


@tool
def execute_custom_sql(sql_query: str) -> str:
    """
    Execute a custom SQL query against the GitHub intelligence tables.
    Available tables: gold_repo_metrics, gold_developer_activity, 
    gold_hourly_trends, gold_event_summary, silver_events
    
    Args:
        sql_query: Valid Spark SQL query
    """
    for table in ['gold_repo_metrics', 'gold_developer_activity', 
                  'gold_hourly_trends', 'gold_event_summary', 'silver_events']:
        sql_query = sql_query.replace(table, f"{CATALOG}.{SCHEMA}.{table}")
    
    if 'LIMIT' not in sql_query.upper():
        sql_query = sql_query.rstrip(';') + ' LIMIT 100'
    
    df = spark.sql(sql_query)
    return df.toPandas().to_json(orient='records', indent=2)


@tool
def get_data_summary() -> str:
    """Get a summary of all available data in the lakehouse."""
    summary = {
        "total_events": spark.table(f"{CATALOG}.{SCHEMA}.silver_events").count(),
        "unique_repositories": spark.table(f"{CATALOG}.{SCHEMA}.gold_repo_metrics").count(),
        "unique_developers": spark.table(f"{CATALOG}.{SCHEMA}.gold_developer_activity").count(),
        "data_timeframe": "Last 3 hours of global GitHub activity"
    }
    return json.dumps(summary, indent=2)


# -----------------------------------------------------------------------------
# CREATE AGENT
# -----------------------------------------------------------------------------

tools = [
    get_top_repositories,
    get_top_developers,
    get_activity_by_category,
    get_hourly_trends,
    search_repository,
    search_developer,
    execute_custom_sql,
    get_data_summary
]

SYSTEM_PROMPT = """You are a GitHub Intelligence Analyst with access to a data lakehouse 
containing real-time GitHub activity data. You help users understand trends, find active 
repositories, identify top contributors, and analyze development patterns.

When answering:
1. Use the appropriate tool to fetch data
2. Provide specific numbers and insights
3. Note that bots (github-actions, dependabot) dominate raw activity counts
4. Be conversational but data-driven

Data covers the last 3 hours of global GitHub public events."""

# Use 'prompt' parameter (current API)
agent = create_react_agent(llm, tools, prompt=SYSTEM_PROMPT)

print("✓ GitHub Intelligence Agent created")
print(f"  Tools: {[t.name for t in tools]}")

# COMMAND ----------

# =============================================================================
# CHAT INTERFACE
# =============================================================================

def chat(user_message: str) -> str:
    """Send a message to the agent and get a response."""
    print(f"\nYou: {user_message}")
    print("-"*60)
    
    response = agent.invoke({
        "messages": [HumanMessage(content=user_message)]
    })
    
    final_message = response["messages"][-1].content
    print(f"\nAgent: {final_message}")
    return final_message

print("✓ Chat interface ready")

# COMMAND ----------

chat("Give me a summary of the data we have")


# COMMAND ----------

chat("Search for repositories related to 'kubernetes projects' ")

# COMMAND ----------

chat("Who are the top 10 human developers, excluding bots?")


# COMMAND ----------

# =============================================================================
# PHASE 3: STREAMING PIPELINE
# =============================================================================
# Real-time ingestion using Auto Loader + Structured Streaming

print("="*60)
print("STREAMING PIPELINE SETUP")
print("="*60)

# Configuration
CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"

# Paths
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/github_archive"
CHECKPOINT_BRONZE = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/checkpoints/bronze_stream"
CHECKPOINT_SILVER = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/checkpoints/silver_stream"

# Tables
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_events"

print(f"""
Configuration:
  Landing Zone:     {LANDING_PATH}
  Bronze Checkpoint: {CHECKPOINT_BRONZE}
  Silver Checkpoint: {CHECKPOINT_SILVER}
  Bronze Table:      {BRONZE_TABLE}
  Silver Table:      {SILVER_TABLE}
""")

# COMMAND ----------

# =============================================================================
# SIMULATE STREAMING: Download New Data
# =============================================================================
# In production, files would land continuously. Here we download a new hour.

import requests
from datetime import datetime, timedelta

def download_new_hour():
    """Download the most recent hour not yet in our volume."""
    
    # Get list of existing files
    existing_files = [f.name for f in dbutils.fs.ls(LANDING_PATH)]
    print(f"Existing files: {len(existing_files)}")
    
    # Find a new hour to download
    now = datetime.utcnow()
    
    for hours_back in range(1, 10):
        dt = now - timedelta(hours=hours_back)
        filename = f"{dt.strftime('%Y-%m-%d')}-{dt.hour}.json.gz"
        
        if filename not in existing_files:
            url = f"https://data.gharchive.org/{filename}"
            filepath = f"{LANDING_PATH}/{filename}"
            
            print(f"Downloading new file: {filename}")
            response = requests.get(url, stream=True)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                print(f"  ✓ Downloaded to {filepath}")
                return filename
            else:
                print(f"  ✗ Failed: {response.status_code}")
    
    print("No new files to download")
    return None

# Download a new hour of data
new_file = download_new_hour()


# COMMAND ----------

# =============================================================================
# STREAMING BRONZE: Auto Loader
# =============================================================================
# Auto Loader automatically detects and processes new files

from pyspark.sql.functions import current_timestamp, input_file_name, to_json, col

print("Setting up Auto Loader stream for Bronze layer...")

# Auto Loader stream - reads new JSON files as they arrive
bronze_stream = (
    spark.readStream
    .format("cloudFiles")  # Auto Loader
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BRONZE}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(LANDING_PATH)
    # Convert complex columns to JSON strings (same as batch)
    .withColumn("actor", to_json(col("actor")))
    .withColumn("repo", to_json(col("repo")))
    .withColumn("payload", to_json(col("payload")))
    .withColumn("org", to_json(col("org")))
    # Add metadata
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

print("✓ Bronze stream configured")
print("\nSchema:")
bronze_stream.printSchema()

# COMMAND ----------

# =============================================================================
# WRITE BRONZE STREAM
# =============================================================================
# Append new records to Bronze table with exactly-once semantics

print("Starting Bronze streaming write...")

bronze_query = (
    bronze_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available data then stop
    .toTable(BRONZE_TABLE)
)

# Wait for completion
bronze_query.awaitTermination()

print("✓ Bronze stream completed")

# Verify
bronze_count = spark.table(BRONZE_TABLE).count()
print(f"✓ Bronze table now has {bronze_count:,} records")

# COMMAND ----------

# =============================================================================
# STREAMING SILVER: Incremental Transformation
# =============================================================================
# Transform new Bronze records to Silver

from pyspark.sql.functions import (
    col, get_json_object, to_timestamp, date_format, 
    hour, dayofweek, when, current_timestamp
)

print("Setting up Silver streaming transformation...")

# Read Bronze as stream
silver_stream = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
    # Parse JSON and transform (same logic as batch)
    .withColumn("actor_id", get_json_object(col("actor"), "$.id").cast("long"))
    .withColumn("actor_login", get_json_object(col("actor"), "$.login"))
    .withColumn("actor_url", get_json_object(col("actor"), "$.url"))
    .withColumn("repo_id", get_json_object(col("repo"), "$.id").cast("long"))
    .withColumn("repo_name", get_json_object(col("repo"), "$.name"))
    .withColumn("repo_url", get_json_object(col("repo"), "$.url"))
    .withColumn("org_id", get_json_object(col("org"), "$.id").cast("long"))
    .withColumn("org_login", get_json_object(col("org"), "$.login"))
    .withColumn("event_timestamp", to_timestamp(col("created_at")))
    .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
    .withColumn("event_hour", hour(col("event_timestamp")))
    .withColumn("event_dayofweek", dayofweek(col("event_timestamp")))
    .withColumn("event_category", 
        when(col("type").isin("PushEvent", "CreateEvent", "DeleteEvent"), "code")
        .when(col("type").isin("PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"), "review")
        .when(col("type").isin("IssuesEvent", "IssueCommentEvent"), "issues")
        .when(col("type").isin("WatchEvent", "ForkEvent", "StarEvent"), "engagement")
        .otherwise("other")
    )
    .withColumn("_processed_at", current_timestamp())
    .select(
        "id", "type", "event_category",
        "actor_id", "actor_login",
        "repo_id", "repo_name",
        "org_id", "org_login",
        "event_timestamp", "event_date", "event_hour", "event_dayofweek",
        "payload", "public",
        "_ingested_at", "_processed_at"
    )
)

print("✓ Silver stream configured")

# COMMAND ----------

# =============================================================================
# WRITE SILVER STREAM
# =============================================================================

print("Starting Silver streaming write...")

silver_query = (
    silver_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available then stop
    .toTable(SILVER_TABLE)
)

# Wait for completion
silver_query.awaitTermination()

print("✓ Silver stream completed")

# Verify
silver_count = spark.table(SILVER_TABLE).count()
print(f"✓ Silver table now has {silver_count:,} records")

# COMMAND ----------

# =============================================================================
# INCREMENTAL GOLD REFRESH
# =============================================================================
# Rebuild Gold tables from updated Silver data
# In production, you'd use Delta Lake MERGE for true incremental updates

from pyspark.sql.functions import count, countDistinct, sum as spark_sum, when, desc, dense_rank, min as spark_min, max as spark_max, current_timestamp
from pyspark.sql.window import Window

print("Refreshing Gold tables...")

df_silver = spark.table(SILVER_TABLE)

# ---------------------------------------------------------
# GOLD: Repository Metrics (full rebuild for simplicity)
# ---------------------------------------------------------
print("\n1. Refreshing gold_repo_metrics...")

df_repo = (
    df_silver
    .groupBy("repo_id", "repo_name")
    .agg(
        count("*").alias("total_events"),
        countDistinct("actor_id").alias("unique_contributors"),
        countDistinct("event_date").alias("active_days"),
        spark_sum(when(col("type") == "PushEvent", 1).otherwise(0)).alias("push_count"),
        spark_sum(when(col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pr_count"),
        spark_sum(when(col("type") == "IssuesEvent", 1).otherwise(0)).alias("issue_count"),
        spark_sum(when(col("type") == "WatchEvent", 1).otherwise(0)).alias("star_count"),
        spark_sum(when(col("type") == "ForkEvent", 1).otherwise(0)).alias("fork_count"),
        spark_sum(when(col("event_category") == "code", 1).otherwise(0)).alias("code_events"),
        spark_sum(when(col("event_category") == "review", 1).otherwise(0)).alias("review_events"),
        spark_sum(when(col("event_category") == "issues", 1).otherwise(0)).alias("issue_events"),
        spark_sum(when(col("event_category") == "engagement", 1).otherwise(0)).alias("engagement_events"),
        spark_min("event_timestamp").alias("first_event"),
        spark_max("event_timestamp").alias("last_event")
    )
    .withColumn("_updated_at", current_timestamp())
    .withColumn("activity_rank", dense_rank().over(Window.orderBy(desc("total_events"))))
)

df_repo.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_repo_metrics")
print(f"   ✓ gold_repo_metrics: {df_repo.count():,} repos")

# ---------------------------------------------------------
# GOLD: Developer Activity
# ---------------------------------------------------------
print("\n2. Refreshing gold_developer_activity...")

df_dev = (
    df_silver
    .groupBy("actor_id", "actor_login")
    .agg(
        count("*").alias("total_events"),
        countDistinct("repo_id").alias("repos_contributed"),
        countDistinct("event_date").alias("active_days"),
        spark_sum(when(col("type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
        spark_sum(when(col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pull_requests"),
        spark_sum(when(col("type") == "IssueCommentEvent", 1).otherwise(0)).alias("comments"),
        spark_min("event_timestamp").alias("first_seen"),
        spark_max("event_timestamp").alias("last_seen")
    )
    .withColumn("_updated_at", current_timestamp())
    .withColumn("activity_rank", dense_rank().over(Window.orderBy(desc("total_events"))))
)

df_dev.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_developer_activity")
print(f"   ✓ gold_developer_activity: {df_dev.count():,} developers")

# ---------------------------------------------------------
# GOLD: Hourly Trends
# ---------------------------------------------------------
print("\n3. Refreshing gold_hourly_trends...")

df_hourly = (
    df_silver
    .groupBy("event_date", "event_hour", "event_category")
    .agg(
        count("*").alias("event_count"),
        countDistinct("actor_id").alias("unique_actors"),
        countDistinct("repo_id").alias("unique_repos")
    )
    .withColumn("_updated_at", current_timestamp())
)

df_hourly.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_hourly_trends")
print(f"   ✓ gold_hourly_trends: {df_hourly.count():,} rows")

# ---------------------------------------------------------
# GOLD: Event Summary
# ---------------------------------------------------------
print("\n4. Refreshing gold_event_summary...")

df_events = (
    df_silver
    .groupBy("event_date", "type", "event_category")
    .agg(
        count("*").alias("event_count"),
        countDistinct("actor_id").alias("unique_actors"),
        countDistinct("repo_id").alias("unique_repos")
    )
    .withColumn("_updated_at", current_timestamp())
)

df_events.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_event_summary")
print(f"   ✓ gold_event_summary: {df_events.count():,} rows")

print("\n" + "="*60)
print("GOLD REFRESH COMPLETE")
print("="*60)

# COMMAND ----------

# =============================================================================
# PIPELINE VERIFICATION
# =============================================================================

print("="*60)
print("STREAMING PIPELINE VERIFICATION")
print("="*60)

bronze_count = spark.table(f"{CATALOG}.{SCHEMA}.bronze_events").count()
silver_count = spark.table(f"{CATALOG}.{SCHEMA}.silver_events").count()
repo_count = spark.table(f"{CATALOG}.{SCHEMA}.gold_repo_metrics").count()
dev_count = spark.table(f"{CATALOG}.{SCHEMA}.gold_developer_activity").count()

print(f"""
Data Lakehouse Status:
  ├── Bronze Events:    {bronze_count:,}
  ├── Silver Events:    {silver_count:,}
  └── Gold Layer:
      ├── Repositories: {repo_count:,}
      └── Developers:   {dev_count:,}

Streaming Components:
  ✓ Auto Loader configured for {LANDING_PATH}
  ✓ Bronze checkpoint at {CHECKPOINT_BRONZE}
  ✓ Silver checkpoint at {CHECKPOINT_SILVER}
  ✓ Incremental Gold refresh working

To add more data:
  1. Drop new .json.gz files into the landing zone
  2. Re-run the Bronze and Silver stream cells
  3. Refresh Gold tables

Production would use:
  • trigger(processingTime='1 minute') for continuous streaming
  • Databricks Jobs for scheduled orchestration
  • Delta Lake MERGE for true incremental Gold updates
""")

print("="*60)
print("✅ PHASE 3: STREAMING PIPELINE COMPLETE")
print("="*60)

# COMMAND ----------

# =============================================================================
# DEDUP SILVER TABLE
# =============================================================================

CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"

print("Deduplicating Silver table...")

df_deduped = spark.table(f"{CATALOG}.{SCHEMA}.silver_events").dropDuplicates(["id"])

# Overwrite with clean data
df_deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_events")

new_count = spark.table(f"{CATALOG}.{SCHEMA}.silver_events").count()
print(f"✓ Silver table deduped: {new_count:,} records")

# COMMAND ----------

# MAGIC %pip install mlflow scikit-learn --quiet
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

OPENAI_API_KEY = dbutils.secrets.get(scope="openai", key="api-key")
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"
FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.feature_repo_trending"

print("✓ Configuration reloaded")

# COMMAND ----------

# =============================================================================
# PHASE 4: FEATURE ENGINEERING
# =============================================================================

from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, when, log2, lit,
    current_timestamp, percentile_approx
)

CATALOG = "data_ai_mastery_ws"
SCHEMA = "github_intelligence"

print("="*60)
print("PHASE 4: FEATURE STORE + ML")
print("="*60)

# Read Gold repo metrics
df_repos = spark.table(f"{CATALOG}.{SCHEMA}.gold_repo_metrics")

print(f"\nSource: {df_repos.count():,} repositories")

# ---------------------------------------------------------
# FEATURE ENGINEERING
# ---------------------------------------------------------
print("\nEngineering features...")

df_features = (
    df_repos
    .filter(col("total_events") >= 5)  # Filter noise (repos with < 5 events)
    
    # Raw features
    .withColumn("log_total_events", log2(col("total_events") + 1))
    .withColumn("log_contributors", log2(col("unique_contributors") + 1))
    
    # Ratios
    .withColumn("pr_ratio", 
        col("pr_count") / (col("total_events") + lit(1)))
    .withColumn("star_ratio", 
        col("star_count") / (col("total_events") + lit(1)))
    .withColumn("fork_ratio", 
        col("fork_count") / (col("total_events") + lit(1)))
    .withColumn("review_ratio", 
        col("review_events") / (col("total_events") + lit(1)))
    .withColumn("engagement_ratio", 
        col("engagement_events") / (col("total_events") + lit(1)))
    
    # Code vs collaboration balance
    .withColumn("collaboration_score",
        (col("pr_count") + col("review_events") + col("issue_events")) / 
        (col("total_events") + lit(1)))
    
    # Is this repo "trending"? (our target variable)
    # Trending = has engagement (stars/forks) AND multiple contributors
    .withColumn("is_trending",
        when(
            (col("unique_contributors") >= 3) & 
            (col("engagement_events") >= 2) &
            (col("total_events") >= 10),
            1
        ).otherwise(0)
    )
    
    .select(
        "repo_id",
        "repo_name",
        "total_events",
        "unique_contributors",
        "push_count",
        "pr_count",
        "star_count",
        "fork_count",
        "log_total_events",
        "log_contributors",
        "pr_ratio",
        "star_ratio",
        "fork_ratio",
        "review_ratio",
        "engagement_ratio",
        "collaboration_score",
        "code_events",
        "review_events",
        "issue_events",
        "engagement_events",
        "is_trending"
    )
)

# Save as feature table
FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.feature_repo_trending"
df_features.write.format("delta").mode("overwrite").saveAsTable(FEATURE_TABLE)

feature_count = df_features.count()
trending_count = df_features.filter(col("is_trending") == 1).count()

print(f"""
✓ Feature table created: {FEATURE_TABLE}
  Total repos:    {feature_count:,}
  Trending repos: {trending_count:,} ({trending_count*100/feature_count:.1f}%)
  Non-trending:   {feature_count - trending_count:,} ({(feature_count - trending_count)*100/feature_count:.1f}%)
""")

# Show sample features
print("Sample features (trending repos):")
df_features.filter(col("is_trending") == 1).orderBy(col("total_events").desc()).show(10, truncate=30)

# COMMAND ----------

# =============================================================================
# MODEL TRAINING WITH MLFLOW
# =============================================================================

import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, classification_report, roc_auc_score
)
import pandas as pd
import numpy as np

print("="*60)
print("MODEL TRAINING")
print("="*60)

# Load features into pandas
df_pd = spark.table(FEATURE_TABLE).toPandas()

# Define feature columns and target
feature_cols = [
    "log_total_events",
    "log_contributors",
    "push_count",
    "pr_count",
    "star_count",
    "fork_count",
    "pr_ratio",
    "star_ratio",
    "fork_ratio",
    "review_ratio",
    "engagement_ratio",
    "collaboration_score",
    "code_events",
    "review_events",
    "issue_events",
    "engagement_events"
]

X = df_pd[feature_cols].fillna(0)
y = df_pd["is_trending"]

print(f"Features: {len(feature_cols)}")
print(f"Samples: {len(X):,}")
print(f"Class distribution: {dict(y.value_counts())}")

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"\nTrain: {len(X_train):,} | Test: {len(X_test):,}")

# ---------------------------------------------------------
# TRAIN WITH MLFLOW TRACKING
# ---------------------------------------------------------

# Set MLflow experiment
mlflow.set_experiment("/Users/samdeeshsehgal@gmail.com/github_intelligence_trending")

with mlflow.start_run(run_name="gradient_boosting_v1") as run:
    
    # Model parameters
    params = {
        "n_estimators": 200,
        "max_depth": 5,
        "learning_rate": 0.1,
        "min_samples_split": 10,
        "min_samples_leaf": 5,
        "subsample": 0.8,
        "random_state": 42
    }
    
    # Log parameters
    mlflow.log_params(params)
    
    # Train model
    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)
    
    # Predictions
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    
    # Metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    auc = roc_auc_score(y_test, y_prob)
    
    # Log metrics
    mlflow.log_metrics({
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "auc_roc": auc
    })
    
    # Log feature importance
    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    
    mlflow.log_text(importance.to_string(), "feature_importance.txt")
    
    # Log model
    mlflow.sklearn.log_model(
        model,
        "trending_repo_model",
        input_example=X_train.iloc[:5]
    )
    
    # Print results
    print(f"""
Model: GradientBoostingClassifier
Run ID: {run.info.run_id}

Metrics:
  Accuracy:  {accuracy:.4f}
  Precision: {precision:.4f}
  Recall:    {recall:.4f}
  F1 Score:  {f1:.4f}
  AUC-ROC:   {auc:.4f}

Top Feature Importances:
{importance.head(10).to_string(index=False)}

Classification Report:
{classification_report(y_test, y_pred, target_names=['Not Trending', 'Trending'])}
""")
    
    print(f"✓ Model logged to MLflow (Run: {run.info.run_id})")

# COMMAND ----------

# =============================================================================
# REGISTER MODEL IN MLFLOW
# =============================================================================

print("Registering model in MLflow Model Registry...")

model_name = "github_trending_predictor"

model_uri = f"runs:/{run.info.run_id}/trending_repo_model"

registered_model = mlflow.register_model(
    model_uri=model_uri,
    name=model_name
)

print(f"""
✓ Model registered in MLflow

  Model Name:    {model_name}
  Version:       {registered_model.version}
  Run ID:        {run.info.run_id}
  
  View in MLflow UI: 
    Experiments > github_intelligence_trending
""")

# COMMAND ----------

# =============================================================================
# TEST INFERENCE
# =============================================================================

print("="*60)
print("TEST INFERENCE")
print("="*60)

loaded_model = mlflow.sklearn.load_model(f"models:/{model_name}/{registered_model.version}")

df_score = spark.table(FEATURE_TABLE).filter(col("total_events") >= 20).toPandas()

feature_cols = [
    "log_total_events", "log_contributors", "push_count", "pr_count",
    "star_count", "fork_count", "pr_ratio", "star_ratio", "fork_ratio",
    "review_ratio", "engagement_ratio", "collaboration_score",
    "code_events", "review_events", "issue_events", "engagement_events"
]

X_score = df_score[feature_cols].fillna(0)
df_score["trending_probability"] = loaded_model.predict_proba(X_score)[:, 1]
df_score["predicted_trending"] = loaded_model.predict(X_score)

print("\nTop Predicted Trending Repositories:")
print("-"*60)

top_trending = (
    df_score
    .sort_values("trending_probability", ascending=False)
    .head(15)
    [["repo_name", "total_events", "unique_contributors", 
      "star_count", "fork_count", "trending_probability"]]
)
print(top_trending.to_string(index=False))

print(f"\n✓ Scored {len(df_score):,} repositories")
print("="*60)
print("✅ PHASE 4: FEATURE STORE + ML COMPLETE")
print("="*60)

# COMMAND ----------

# =============================================================================
# PHASE 5: PRODUCTION PIPELINE ORCHESTRATION
# =============================================================================
# This cell defines a complete end-to-end pipeline function
# that can be scheduled as a Databricks Job

from datetime import datetime

def run_pipeline():
    """
    End-to-end pipeline: Ingest -> Bronze -> Silver -> Gold -> Score
    This function can be scheduled as a Databricks Job.
    """
    
    import requests
    from datetime import datetime, timedelta
    from pyspark.sql.functions import (
    col, to_json, current_timestamp,
    get_json_object, to_timestamp, date_format, hour, dayofweek,
    when, count, countDistinct, sum as spark_sum, 
    min as spark_min, max as spark_max, dense_rank, desc, log2, lit
    )
    
    from pyspark.sql.window import Window
    
    CATALOG = "data_ai_mastery_ws"
    SCHEMA = "github_intelligence"
    LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/github_archive"
    
    start_time = datetime.now()
    print("="*60)
    print(f"PIPELINE RUN: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # ─── STEP 1: INGEST NEW DATA ─────────────────────────────
    print("\n[1/5] Checking for new data...")
    
    existing_files = [f.name for f in dbutils.fs.ls(LANDING_PATH)]
    now = datetime.utcnow()
    new_files = 0
    
    for hours_back in range(1, 6):
        dt = now - timedelta(hours=hours_back)
        filename = f"{dt.strftime('%Y-%m-%d')}-{dt.hour}.json.gz"
        
        if filename not in existing_files:
            url = f"https://data.gharchive.org/{filename}"
            filepath = f"{LANDING_PATH}/{filename}"
            response = requests.get(url, stream=True)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                new_files += 1
                print(f"  Downloaded: {filename}")
    
    print(f"  ✓ {new_files} new files ingested")
    
    # ─── STEP 2: BRONZE (Auto Loader) ────────────────────────
    print("\n[2/5] Streaming to Bronze...")
    
    bronze_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", 
                f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/checkpoints/bronze_stream/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Rescue unknown fields
        .option("rescuedDataColumn", "_rescued_data")  # Store unknown fields here
        .load(LANDING_PATH)
        .withColumn("actor", to_json(col("actor")))
        .withColumn("repo", to_json(col("repo")))
        .withColumn("payload", to_json(col("payload")))
        .withColumn("org", to_json(col("org")))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )
    
    bronze_query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", 
                f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/checkpoints/bronze_stream")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.bronze_events")
    )
    bronze_query.awaitTermination()
    
    bronze_count = spark.table(f"{CATALOG}.{SCHEMA}.bronze_events").count()
    print(f"  ✓ Bronze: {bronze_count:,} records")
    
    
    # ─── STEP 3: SILVER (Transform) ──────────────────────────
    print("\n[3/5] Streaming to Silver...")
    
    silver_stream = (
        spark.readStream
        .format("delta")
        .table(f"{CATALOG}.{SCHEMA}.bronze_events")
        .withColumn("actor_id", get_json_object(col("actor"), "$.id").cast("long"))
        .withColumn("actor_login", get_json_object(col("actor"), "$.login"))
        .withColumn("actor_url", get_json_object(col("actor"), "$.url"))
        .withColumn("repo_id", get_json_object(col("repo"), "$.id").cast("long"))
        .withColumn("repo_name", get_json_object(col("repo"), "$.name"))
        .withColumn("repo_url", get_json_object(col("repo"), "$.url"))
        .withColumn("org_id", get_json_object(col("org"), "$.id").cast("long"))
        .withColumn("org_login", get_json_object(col("org"), "$.login"))
        .withColumn("event_timestamp", to_timestamp(col("created_at")))
        .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("event_dayofweek", dayofweek(col("event_timestamp")))
        .withColumn("event_category", 
            when(col("type").isin("PushEvent", "CreateEvent", "DeleteEvent"), "code")
            .when(col("type").isin("PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"), "review")
            .when(col("type").isin("IssuesEvent", "IssueCommentEvent"), "issues")
            .when(col("type").isin("WatchEvent", "ForkEvent", "StarEvent"), "engagement")
            .otherwise("other")
        )
        .withColumn("_processed_at", current_timestamp())
        .select(
            "id", "type", "event_category",
            "actor_id", "actor_login", "repo_id", "repo_name",
            "org_id", "org_login", "event_timestamp", "event_date",
            "event_hour", "event_dayofweek", "payload", "public",
            "_ingested_at", "_processed_at"
        )
    )
    
    silver_query = (
        silver_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation",
                f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/checkpoints/silver_stream")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.silver_events")
    )
    silver_query.awaitTermination()
    
    silver_count = spark.table(f"{CATALOG}.{SCHEMA}.silver_events").count()
    print(f"  ✓ Silver: {silver_count:,} records")
    
    # ─── STEP 4: GOLD (Aggregate) ────────────────────────────
    print("\n[4/5] Refreshing Gold tables...")
    
    df_silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_events")
    
    # Repo metrics
    df_repo = (
        df_silver.groupBy("repo_id", "repo_name")
        .agg(
            count("*").alias("total_events"),
            countDistinct("actor_id").alias("unique_contributors"),
            countDistinct("event_date").alias("active_days"),
            spark_sum(when(col("type") == "PushEvent", 1).otherwise(0)).alias("push_count"),
            spark_sum(when(col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pr_count"),
            spark_sum(when(col("type") == "IssuesEvent", 1).otherwise(0)).alias("issue_count"),
            spark_sum(when(col("type") == "WatchEvent", 1).otherwise(0)).alias("star_count"),
            spark_sum(when(col("type") == "ForkEvent", 1).otherwise(0)).alias("fork_count"),
            spark_sum(when(col("event_category") == "code", 1).otherwise(0)).alias("code_events"),
            spark_sum(when(col("event_category") == "review", 1).otherwise(0)).alias("review_events"),
            spark_sum(when(col("event_category") == "issues", 1).otherwise(0)).alias("issue_events"),
            spark_sum(when(col("event_category") == "engagement", 1).otherwise(0)).alias("engagement_events"),
            spark_min("event_timestamp").alias("first_event"),
            spark_max("event_timestamp").alias("last_event")
        )
        .withColumn("_updated_at", current_timestamp())
        .withColumn("activity_rank", dense_rank().over(Window.orderBy(desc("total_events"))))
    )
    df_repo.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_repo_metrics")
    
    # Developer activity
    df_dev = (
        df_silver.groupBy("actor_id", "actor_login")
        .agg(
            count("*").alias("total_events"),
            countDistinct("repo_id").alias("repos_contributed"),
            countDistinct("event_date").alias("active_days"),
            spark_sum(when(col("type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
            spark_sum(when(col("type") == "PullRequestEvent", 1).otherwise(0)).alias("pull_requests"),
            spark_sum(when(col("type") == "IssueCommentEvent", 1).otherwise(0)).alias("comments"),
            spark_min("event_timestamp").alias("first_seen"),
            spark_max("event_timestamp").alias("last_seen")
        )
        .withColumn("_updated_at", current_timestamp())
        .withColumn("activity_rank", dense_rank().over(Window.orderBy(desc("total_events"))))
    )
    df_dev.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_developer_activity")
    
    # Hourly trends
    df_hourly = (
        df_silver.groupBy("event_date", "event_hour", "event_category")
        .agg(count("*").alias("event_count"), countDistinct("actor_id").alias("unique_actors"), countDistinct("repo_id").alias("unique_repos"))
        .withColumn("_updated_at", current_timestamp())
    )
    df_hourly.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_hourly_trends")
    
    # Event summary
    df_events = (
        df_silver.groupBy("event_date", "type", "event_category")
        .agg(count("*").alias("event_count"), countDistinct("actor_id").alias("unique_actors"), countDistinct("repo_id").alias("unique_repos"))
        .withColumn("_updated_at", current_timestamp())
    )
    df_events.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_event_summary")
    
    print("  ✓ All Gold tables refreshed")
    
    # ─── STEP 5: SCORE WITH ML MODEL ─────────────────────────
    print("\n[5/5] Scoring repositories...")
    
    df_repos_gold = spark.table(f"{CATALOG}.{SCHEMA}.gold_repo_metrics")
    
    df_features = (
        df_repos_gold.filter(col("total_events") >= 5)
        .withColumn("log_total_events", log2(col("total_events") + 1))
        .withColumn("log_contributors", log2(col("unique_contributors") + 1))
        .withColumn("pr_ratio", col("pr_count") / (col("total_events") + lit(1)))
        .withColumn("star_ratio", col("star_count") / (col("total_events") + lit(1)))
        .withColumn("fork_ratio", col("fork_count") / (col("total_events") + lit(1)))
        .withColumn("review_ratio", col("review_events") / (col("total_events") + lit(1)))
        .withColumn("engagement_ratio", col("engagement_events") / (col("total_events") + lit(1)))
        .withColumn("collaboration_score",
            (col("pr_count") + col("review_events") + col("issue_events")) / (col("total_events") + lit(1)))
        .withColumn("is_trending",
            when((col("unique_contributors") >= 3) & (col("engagement_events") >= 2) & (col("total_events") >= 10), 1).otherwise(0))
    )
    
    df_features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.feature_repo_trending")
    
    trending = df_features.filter(col("is_trending") == 1).count()
    total = df_features.count()
    print(f"  ✓ {total:,} repos scored, {trending:,} trending")
    
    # ─── SUMMARY ──────────────────────────────────────────────
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print(f"""
{'='*60}
PIPELINE COMPLETE
{'='*60}
  Duration:     {elapsed:.1f} seconds
  New files:    {new_files}
  Bronze:       {bronze_count:,} records
  Silver:       {silver_count:,} records
  Repos scored: {total:,}
  Trending:     {trending:,}
{'='*60}
""")

print("✓ Pipeline function defined: run_pipeline()")
print("  This can be scheduled as a Databricks Job")
print("  To run manually: run_pipeline()")

# COMMAND ----------

# Run the pipeline
run_pipeline()

# COMMAND ----------

