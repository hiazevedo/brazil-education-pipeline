# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — ENEM
# MAGIC Reads raw Parquet files from the Volume and writes to `bronze.enem_raw`.
# MAGIC - No transformations — data is kept exactly as received from INEP
# MAGIC - Schema-on-read with all columns as strings
# MAGIC - Adds `_ingested_at` audit column

# COMMAND ----------

from pyspark.sql import functions as F
from brazil_education_pipeline.config import VOLUME_RAW, BRONZE_ENEM

# COMMAND ----------

# MAGIC %md ## Read raw Parquet files

# COMMAND ----------

df_raw = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(f"{VOLUME_RAW}/enem_*.parquet")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

print(f"Rows: {df_raw.count():,} | Columns: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md ## Write to Delta — append (idempotent via MERGE or overwrite partition)

# COMMAND ----------

(
    df_raw.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO")
    .saveAsTable(BRONZE_ENEM)
)

print(f"[OK] Written to {BRONZE_ENEM}")

# COMMAND ----------

display(spark.sql(f"SELECT NU_ANO, COUNT(*) AS qt FROM {BRONZE_ENEM} GROUP BY 1 ORDER BY 1"))
