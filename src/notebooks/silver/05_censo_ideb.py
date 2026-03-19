# Databricks notebook source

# MAGIC %md
# MAGIC # Silver — Censo Escolar + IDEB
# MAGIC Cleans both bronze tables and produces `silver.escolas` and `silver.ideb`.
# MAGIC
# MAGIC **silver.escolas**: typed school records with an `infra_score` (0–5)
# MAGIC **silver.ideb**: pivoted IDEB series ready to join with school/municipality data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from brazil_education_pipeline.config import (
    BRONZE_CENSO_ESCOLAR, BRONZE_IDEB,
    SILVER_ESCOLAS, SILVER_IDEB,
)

# COMMAND ----------

# MAGIC %md ## Silver — Escolas

# COMMAND ----------

INFRA_COLS = [
    "IN_ENERGIA_REDE_PUBLICA",
    "IN_AGUA_POTAVEL",
    "IN_BIBLIOTECA",
    "IN_LABORATORIO_INFORMATICA",
    "IN_INTERNET",
]

df_censo = spark.table(BRONZE_CENSO_ESCOLAR)

INT_COLS = ["CO_ENTIDADE", "CO_MUNICIPIO", "CO_UF", "TP_DEPENDENCIA",
            "TP_LOCALIZACAO", "QT_MAT_BAS", "QT_DOC_BAS"] + INFRA_COLS

df_escolas = df_censo
for col in INT_COLS:
    df_escolas = df_escolas.withColumn(col, F.col(col).cast(IntegerType()))

# infra_score: sum of 5 binary infrastructure indicators (0–5)
df_escolas = df_escolas.withColumn(
    "infra_score",
    sum(F.coalesce(F.col(c), F.lit(0)) for c in INFRA_COLS)
)

(
    df_escolas.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO_CENSO")
    .saveAsTable(SILVER_ESCOLAS)
)
print(f"[OK] Written to {SILVER_ESCOLAS}")

# COMMAND ----------

# MAGIC %md ## Silver — IDEB

# COMMAND ----------

df_ideb_raw = spark.table(BRONZE_IDEB)

# INEP IDEB columns follow the pattern VL_OBSERVADO_YYYY (actual IDEB score per edition)
# Extract year from column name and unpivot to tidy format: CO_MUNICIPIO | SEGMENTO | ANO | IDEB
import re

ID_COLS   = ["SG_UF", "CO_MUNICIPIO", "NO_MUNICIPIO", "REDE", "SEGMENTO"]
IDEB_COLS = [c for c in df_ideb_raw.columns if re.match(r"VL_OBSERVADO_\d{4}$", c)]

stack_expr = "stack({}, {}) as (ano, ideb)".format(
    len(IDEB_COLS),
    ", ".join(["'{}', `{}`".format(c.split("_")[-1], c) for c in IDEB_COLS])
)

df_ideb = (
    df_ideb_raw.select(*ID_COLS, F.expr(stack_expr))
    .filter(F.col("ideb").isNotNull())
    .withColumn("ano",  F.col("ano").cast(IntegerType()))
    .withColumn("ideb", F.col("ideb").cast(FloatType()))
    .withColumn("CO_MUNICIPIO", F.col("CO_MUNICIPIO").cast(IntegerType()))
)

(
    df_ideb.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_IDEB)
)
print(f"[OK] Written to {SILVER_IDEB}")
display(spark.sql(f"SELECT SEGMENTO, ano, ROUND(AVG(ideb), 2) AS media_ideb FROM {SILVER_IDEB} GROUP BY 1,2 ORDER BY 1,2"))
