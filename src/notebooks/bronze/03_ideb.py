# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — IDEB
# MAGIC Lê arquivos Parquet brutos do Volume e grava em `bronze.ideb_raw`.
# MAGIC O IDEB é bienal (2019 / 2021 / 2023) e cobre Anos Iniciais (EF1) e Anos Finais (EF2).

# COMMAND ----------

from pyspark.sql import functions as F
CATALOG    = "education_pipeline"
VOLUME_RAW = f"/Volumes/{CATALOG}/bronze/raw_files"
BRONZE_IDEB = f"{CATALOG}.bronze.ideb_raw"

# COMMAND ----------

df_raw = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(f"{VOLUME_RAW}/ideb_*.parquet")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
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
