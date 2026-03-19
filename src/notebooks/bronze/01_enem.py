# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — ENEM
# MAGIC Lê arquivos Parquet brutos do Volume e grava em `bronze.enem_raw`.
# MAGIC - Sem transformações — dados mantidos exatamente como recebidos do INEP
# MAGIC - Schema-on-read com todas as colunas como string
# MAGIC - Adiciona coluna de auditoria `_ingested_at`

# COMMAND ----------

from pyspark.sql import functions as F
from brazil_education_pipeline.config import VOLUME_RAW, BRONZE_ENEM

# COMMAND ----------

# MAGIC %md ## Leitura dos arquivos Parquet brutos

# COMMAND ----------

df_raw = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(f"{VOLUME_RAW}/enem_*.parquet")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

print(f"Rows: {df_raw.count():,} | Columns: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md ## Gravação no Delta — sobrescrita com particionamento por ano

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
