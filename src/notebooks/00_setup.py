# Databricks notebook source

# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Creates the catalog, schemas, and volume used by the entire pipeline.
# MAGIC Run once before deploying other notebooks.

# COMMAND ----------

dbutils.widgets.text("catalog", "education_pipeline")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md ## Catalog & Schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

for schema in ("bronze", "silver", "gold", "ml_features"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"[OK] Schema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Volume (raw file landing zone)

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_files
    COMMENT 'Landing zone for raw ENEM, Censo Escolar and IDEB files uploaded via GitHub Actions'
""")

print(f"[OK] Volume: /Volumes/{catalog}/bronze/raw_files")

# COMMAND ----------

# MAGIC %md ## Validate

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
display(spark.sql(f"SHOW VOLUMES IN {catalog}.bronze"))
