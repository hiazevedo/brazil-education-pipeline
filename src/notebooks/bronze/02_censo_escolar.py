# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Censo Escolar
# MAGIC Lê arquivos Parquet brutos do Volume e grava em `bronze.censo_escolar_raw`.

# COMMAND ----------

from pyspark.sql import functions as F
CATALOG              = "education_pipeline"
VOLUME_RAW           = f"/Volumes/{CATALOG}/bronze/raw_files"
BRONZE_CENSO_ESCOLAR = f"{CATALOG}.bronze.censo_escolar_raw"

# COMMAND ----------

df_raw = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(f"{VOLUME_RAW}/censo_escolar_*.parquet")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

print(f"Rows: {df_raw.count():,} | Columns: {len(df_raw.columns)}")

# COMMAND ----------

(
    df_raw.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO_CENSO")
    .saveAsTable(BRONZE_CENSO_ESCOLAR)
)

print(f"[OK] Written to {BRONZE_CENSO_ESCOLAR}")

# COMMAND ----------

display(spark.sql(f"SELECT NU_ANO_CENSO, COUNT(*) AS qt FROM {BRONZE_CENSO_ESCOLAR} GROUP BY 1 ORDER BY 1"))
