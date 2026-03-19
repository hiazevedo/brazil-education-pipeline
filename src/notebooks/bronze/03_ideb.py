# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — IDEB
# MAGIC Reads raw Parquet files from the Volume and writes to `bronze.ideb_raw`.
# MAGIC IDEB is biennial (2019 / 2021 / 2023) and covers Anos Iniciais (EF1) and Anos Finais (EF2).

# COMMAND ----------

from pyspark.sql import functions as F
from brazil_education_pipeline.config import VOLUME_RAW, BRONZE_IDEB

# COMMAND ----------

df_raw = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(f"{VOLUME_RAW}/ideb_*.parquet")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

print(f"Rows: {df_raw.count():,} | Columns: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

(
    df_raw.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_IDEB)
)

print(f"[OK] Written to {BRONZE_IDEB}")

# COMMAND ----------

display(spark.sql(f"SELECT SEGMENTO, COUNT(*) AS qt FROM {BRONZE_IDEB} GROUP BY 1 ORDER BY 1"))
