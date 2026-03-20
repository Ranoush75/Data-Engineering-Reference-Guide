# =============================================================================
# API Data Pipeline Options — Reference File
# =============================================================================
# Purpose : Template patterns for ingesting data from an API into a data
#           engineering pipeline. Each option is self-contained and can be
#           adapted to any project.
#
# IMPORTANT: Keep all API keys and DB credentials in a separate config/secret
#            file. Never commit credentials to GitHub.
#
# Patterns covered:
#   1. Batch Pull → Delta MERGE             (Databricks)
#   2. Batch Pull → PostgreSQL (CDC)        (any environment)
#   3. Batch Pull → CSV → Delta             (Databricks, with raw file layer)
#   4. Micro-batch Streaming (foreachBatch) (Databricks Streaming)
#   5. Delta Live Tables (DLT)              (Databricks managed pipelines)
#   6. API → Kafka → Spark Structured Streaming
# =============================================================================

import requests
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


# =============================================================================
# OPTION 1: Batch Pull → Delta MERGE (Databricks)
# =============================================================================
# When to use:
#   - Running on Databricks with a Delta Lake target table
#   - Scheduled batch job (e.g., daily or hourly)
#   - Need upsert behavior (update existing + insert new rows)
# -----------------------------------------------------------------------------

def extract_from_api(url, params):
    # Call API and return JSON response as a pandas DataFrame
    response = requests.get(url, params=params)
    data = response.json()
    df = pd.DataFrame(data)
    print(f"[extract] rows fetched: {len(df)}")
    logging.info(f"extract complete: {len(df)} rows")
    return df

def load_to_delta_merge(df, spark, target_table, merge_key):
    # Load pandas DataFrame into Spark, register as temp view, then MERGE into Delta
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("source")
    print(f"[load] merging {len(df)} rows into {target_table} on key: {merge_key}")
    logging.info(f"merge into {target_table} started")
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING source s ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logging.info(f"merge into {target_table} complete")

# --- Usage ---
# df = extract_from_api("https://api.example.com/data", params={"key": "YOUR_KEY"})
# load_to_delta_merge(df, spark, target_table="catalog.bronze.table", merge_key="date")


# =============================================================================
# OPTION 2: Batch Pull → PostgreSQL (CDC Pattern)
# =============================================================================
# When to use:
#   - Target is a PostgreSQL database (not Databricks)
#   - Need Change Data Capture: only insert genuinely new/changed rows
#   - Works in any Python environment (no Spark required)
# -----------------------------------------------------------------------------

def extract_from_api_to_df(url, params):
    # Call API and return reshaped pandas DataFrame
    response = requests.get(url, params=params)
    df = pd.DataFrame(response.json())
    print(f"[extract] rows fetched: {len(df)}")
    logging.info(f"extract complete: {len(df)} rows")
    return df

def load_to_postgres_cdc(new_df, engine, table_name, join_keys):
    # Compare new data against existing table, insert only new/changed rows
    existing_df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    merged = new_df.merge(existing_df, on=join_keys, how="outer", indicator=True)
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    print(f"[load] new rows to insert: {len(new_rows)}")
    logging.info(f"CDC insert: {len(new_rows)} new rows into {table_name}")
    new_rows.to_sql(table_name, engine, if_exists="append", index=False, chunksize=1000)

# --- Usage ---
# from sqlalchemy import create_engine
# engine = create_engine("postgresql://user:password@host:port/dbname")  # use secrets file
# df = extract_from_api_to_df("https://api.example.com/data", params={"key": "YOUR_KEY"})
# load_to_postgres_cdc(df, engine, table_name="ibm_history", join_keys=["date"])


# =============================================================================
# OPTION 3: Batch Pull → CSV → Delta (with Raw File Layer)
# =============================================================================
# When to use:
#   - Want to keep a raw CSV copy in a Volume (Bronze file layer)
#   - Re-process raw files later without re-calling the API
#   - Running on Databricks with Unity Catalog Volumes
# -----------------------------------------------------------------------------

def extract_and_save_csv(url, params, csv_path):
    # Call API, save raw response as CSV to a Databricks Volume
    response = requests.get(url, params=params)
    df = pd.DataFrame(response.json())
    df.to_csv(csv_path, index=False)
    print(f"[extract] saved {len(df)} rows to {csv_path}")
    logging.info(f"raw CSV saved: {csv_path}")
    return csv_path

def load_csv_to_delta(spark, csv_path, target_table, merge_key):
    # Read the saved CSV into Spark and MERGE into Delta table
    spark_df = spark.read.option("header", True).csv(csv_path)
    spark_df.createOrReplaceTempView("source")
    print(f"[load] loading CSV into {target_table}")
    logging.info(f"loading {csv_path} into {target_table}")
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING source s ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logging.info(f"delta load complete: {target_table}")

# --- Usage ---
# extract_and_save_csv("https://api.example.com/data", {"key": "YOUR_KEY"},
#                      "/Volumes/catalog/schema/volume/raw_data.csv")
# load_csv_to_delta(spark, "/Volumes/catalog/schema/volume/raw_data.csv",
#                   "catalog.bronze.table", merge_key="date")


# =============================================================================
# OPTION 4: Micro-batch Streaming with foreachBatch (Databricks)
# =============================================================================
# When to use:
#   - Need near-real-time API polling (every N seconds/minutes)
#   - Want a streaming pipeline but API doesn't push events
#   - Databricks environment with Spark Structured Streaming
# -----------------------------------------------------------------------------

def poll_api_and_merge(batch_df, batch_id, url, params, spark, target_table, merge_key):
    # Called every trigger interval — fetches API and merges results
    response = requests.get(url, params=params)
    df = pd.DataFrame(response.json())
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("source")
    print(f"[batch {batch_id}] rows fetched: {len(df)}")
    logging.info(f"batch {batch_id}: merging {len(df)} rows")
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING source s ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# --- Usage ---
# trigger_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
# query = (
#     trigger_stream.writeStream
#     .foreachBatch(lambda df, id: poll_api_and_merge(
#         df, id,
#         url="https://api.example.com/data",
#         params={"key": "YOUR_KEY"},
#         spark=spark,
#         target_table="catalog.bronze.table",
#         merge_key="date"
#     ))
#     .option("checkpointLocation", "/tmp/checkpoint_api")
#     .trigger(processingTime="5 minutes")
#     .start()
# )
# query.awaitTermination()


# =============================================================================
# OPTION 5: Delta Live Tables (DLT) with API Source (Databricks)
# =============================================================================
# When to use:
#   - Production Databricks pipeline with lineage, monitoring, data quality
#   - Want managed orchestration (auto-retry, dependency graph)
#   - Team environment where observability matters
# -----------------------------------------------------------------------------

# import dlt  # available only inside a DLT pipeline notebook

# @dlt.table(
#     name="bronze_api_data",
#     comment="Raw API ingestion layer"
# )
# def bronze_api_data():
#     # Fetch API data and return as Spark DataFrame for DLT to manage
#     response = requests.get("https://api.example.com/data", params={"key": "YOUR_KEY"})
#     df = pd.DataFrame(response.json())
#     logging.info(f"DLT extract: {len(df)} rows")
#     return spark.createDataFrame(df)

# @dlt.table(
#     name="silver_api_data",
#     comment="Cleaned and typed API data"
# )
# @dlt.expect("valid_date", "date IS NOT NULL")
# def silver_api_data():
#     # Read from Bronze DLT table and apply transformations
#     return dlt.read("bronze_api_data").dropDuplicates(["date"])


# =============================================================================
# OPTION 6: API → Kafka → Spark Structured Streaming
# =============================================================================
# When to use:
#   - High-volume, event-driven, real-time ingestion
#   - API data needs to fan out to multiple consumers
#   - Building a modern streaming architecture (Kafka as backbone)
# -----------------------------------------------------------------------------

# PRODUCER — push API data to Kafka topic (runs on a scheduler or as a service)
# from kafka import KafkaProducer
# import json, time
#
# def api_to_kafka(url, params, topic, bootstrap_servers):
#     # Poll API and publish each record as a Kafka message
#     producer = KafkaProducer(
#         bootstrap_servers=bootstrap_servers,
#         value_serializer=lambda v: json.dumps(v).encode("utf-8")
#     )
#     response = requests.get(url, params=params)
#     records = response.json()
#     for record in records:
#         producer.send(topic, record)
#     producer.flush()
#     print(f"[kafka producer] sent {len(records)} messages to topic: {topic}")
#     logging.info(f"kafka: {len(records)} messages published to {topic}")

# CONSUMER — Spark reads from Kafka and writes to Delta
# kafka_df = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "broker:9092")
#     .option("subscribe", "api_topic")
#     .load()
# )
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StringType, DoubleType
#
# schema = StructType()  # define your schema here
# parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
# parsed_df.writeStream.format("delta") \
#     .option("checkpointLocation", "/tmp/checkpoint_kafka") \
#     .outputMode("append") \
#     .table("catalog.bronze.kafka_table")


# =============================================================================
# SUMMARY
# =============================================================================
#
# Option                          | Mode      | Target       | Incremental | Complexity
# --------------------------------|-----------|--------------|-------------|------------
# 1. Batch Pull → Delta MERGE     | Batch     | Delta Lake   | Yes (MERGE) | Low
# 2. Batch Pull → PostgreSQL CDC  | Batch     | PostgreSQL   | Yes (join)  | Low
# 3. Batch Pull → CSV → Delta     | Batch     | Delta Lake   | Yes (MERGE) | Low
# 4. foreachBatch Streaming       | Streaming | Delta Lake   | Yes (MERGE) | Medium
# 5. Delta Live Tables (DLT)      | Both      | Delta Lake   | Yes         | Medium
# 6. API → Kafka → Spark Stream   | Streaming | Delta Lake   | Yes         | High
#
# Quick decision guide:
#   - Scheduled batch job on Databricks        → Option 1
#   - Scheduled batch job on PostgreSQL        → Option 2
#   - Need a raw file backup before loading    → Option 3
#   - Near-real-time polling (every N min)     → Option 4
#   - Managed production pipeline on Databricks→ Option 5
#   - High-volume real-time event streaming    → Option 6
# =============================================================================
