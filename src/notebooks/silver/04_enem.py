# Databricks notebook source

# MAGIC %md
# MAGIC # Silver — ENEM
# MAGIC Limpa e tipifica `bronze.enem_raw` → `silver.enem`.
# MAGIC - Converte colunas para os tipos corretos
# MAGIC - Remove treineiros (IN_TREINEIRO = 1) e linhas com todas as notas nulas
# MAGIC - Calcula `nota_media` (média das 5 provas)
# MAGIC - Padroniza códigos categóricos para rótulos legíveis

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from brazil_education_pipeline.config import BRONZE_ENEM, SILVER_ENEM

# COMMAND ----------

# MAGIC %md ## Leitura da camada Bronze

# COMMAND ----------

df = spark.table(BRONZE_ENEM)

# COMMAND ----------

# MAGIC %md ## Conversão de tipos

# COMMAND ----------

GRADE_COLS = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]

df_typed = (
    df
    .withColumn("NU_ANO",            F.col("NU_ANO").cast(IntegerType()))
    .withColumn("CO_MUNICIPIO_ESC",  F.col("CO_MUNICIPIO_ESC").cast(IntegerType()))
    .withColumn("TP_FAIXA_ETARIA",   F.col("TP_FAIXA_ETARIA").cast(IntegerType()))
    .withColumn("TP_COR_RACA",       F.col("TP_COR_RACA").cast(IntegerType()))
    .withColumn("TP_ESCOLA",         F.col("TP_ESCOLA").cast(IntegerType()))
    .withColumn("IN_TREINEIRO",      F.col("IN_TREINEIRO").cast(IntegerType()))
    # TP_SEXO (M/F), Q001, Q002, Q006 are letter-coded categoricals — keep as string
)

for col in GRADE_COLS:
    df_typed = df_typed.withColumn(col, F.col(col).cast(FloatType()))

# COMMAND ----------

# MAGIC %md ## Filtro de registros inválidos

# COMMAND ----------

df_clean = (
    df_typed
    .filter(F.col("IN_TREINEIRO") == 0)
    .filter(
        F.col("NU_NOTA_MT").isNotNull() |
        F.col("NU_NOTA_CN").isNotNull() |
        F.col("NU_NOTA_LC").isNotNull() |
        F.col("NU_NOTA_CH").isNotNull()
    )
    .drop("IN_TREINEIRO", "_source_file")
)

# COMMAND ----------

# MAGIC %md ## Cálculo da nota_media

# COMMAND ----------

df_final = df_clean.withColumn(
    "nota_media",
    (
        F.coalesce(F.col("NU_NOTA_CN"), F.lit(0)) +
        F.coalesce(F.col("NU_NOTA_CH"), F.lit(0)) +
        F.coalesce(F.col("NU_NOTA_LC"), F.lit(0)) +
        F.coalesce(F.col("NU_NOTA_MT"), F.lit(0)) +
        F.coalesce(F.col("NU_NOTA_REDACAO"), F.lit(0))
    ) / F.lit(5.0)
)

# COMMAND ----------

# MAGIC %md ## Gravação na camada Silver

# COMMAND ----------

(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO")
    .saveAsTable(SILVER_ENEM)
)

print(f"[OK] Written to {SILVER_ENEM}")
display(spark.sql(f"SELECT NU_ANO, COUNT(*) AS qt, ROUND(AVG(nota_media), 2) AS media FROM {SILVER_ENEM} GROUP BY 1 ORDER BY 1"))
