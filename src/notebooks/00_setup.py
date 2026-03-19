# Databricks notebook source

# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Cria o catálogo, schemas e volume utilizados por todo o pipeline.
# MAGIC Execute uma vez antes de fazer o deploy dos demais notebooks.

# COMMAND ----------

dbutils.widgets.text("catalog", "education_pipeline")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md ## Catálogo e Schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

for schema in ("bronze", "silver", "gold", "ml_features"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"[OK] Schema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Volume (zona de entrada dos arquivos brutos)

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_files
    COMMENT 'Zona de entrada para arquivos brutos do ENEM, Censo Escolar e IDEB enviados via GitHub Actions'
""")

print(f"[OK] Volume: /Volumes/{catalog}/bronze/raw_files")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
display(spark.sql(f"SHOW VOLUMES IN {catalog}.bronze"))
