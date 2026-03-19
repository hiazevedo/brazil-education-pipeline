# Databricks notebook source

# MAGIC %md
# MAGIC # Gold — Analytics
# MAGIC Produces all 5 Gold tables used by the dashboard.
# MAGIC
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `gold.desempenho_por_perfil` | ENEM score by income × race × school type |
# MAGIC | `gold.desigualdade_regional` | Average score by UF with national rank and gap |
# MAGIC | `gold.infraestrutura_vs_ideb` | School infrastructure score correlated with IDEB |
# MAGIC | `gold.evolucao_historica` | Time series 2020–2023 by region |
# MAGIC | `gold.escola_performance_rank` | Public vs private school ranking |

# COMMAND ----------

from pyspark.sql import functions as F, Window
from brazil_education_pipeline.config import (
    SILVER_ENEM, SILVER_ESCOLAS, SILVER_IDEB,
    GOLD_DESEMPENHO_PERFIL, GOLD_DESIGUALDADE_REGIONAL,
    GOLD_INFRA_VS_IDEB, GOLD_EVOLUCAO_HISTORICA, GOLD_ESCOLA_RANK,
)

# COMMAND ----------

enem    = spark.table(SILVER_ENEM)
escolas = spark.table(SILVER_ESCOLAS)
ideb    = spark.table(SILVER_IDEB)

# COMMAND ----------

# MAGIC %md ## 1. Desempenho por Perfil

# COMMAND ----------

(
    enem.groupBy("NU_ANO", "TP_COR_RACA", "TP_ESCOLA", "Q006")
    .agg(
        F.count("*").alias("qt_candidatos"),
        F.round(F.avg("nota_media"), 2).alias("media_geral"),
        F.round(F.avg("NU_NOTA_MT"), 2).alias("media_matematica"),
        F.round(F.avg("NU_NOTA_REDACAO"), 2).alias("media_redacao"),
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_DESEMPENHO_PERFIL)
)
print(f"[OK] {GOLD_DESEMPENHO_PERFIL}")

# COMMAND ----------

# MAGIC %md ## 2. Desigualdade Regional

# COMMAND ----------

w_rank = Window.partitionBy("NU_ANO").orderBy(F.desc("media_geral"))

(
    enem.groupBy("NU_ANO", "SG_UF_ESC")
    .agg(
        F.count("*").alias("qt_candidatos"),
        F.round(F.avg("nota_media"), 2).alias("media_geral"),
    )
    .withColumn("rank_uf", F.rank().over(w_rank))
    .withColumn(
        "gap_vs_media_nacional",
        F.col("media_geral") - F.avg("media_geral").over(Window.partitionBy("NU_ANO"))
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_DESIGUALDADE_REGIONAL)
)
print(f"[OK] {GOLD_DESIGUALDADE_REGIONAL}")

# COMMAND ----------

# MAGIC %md ## 3. Infraestrutura vs IDEB

# COMMAND ----------

escolas_agg = (
    escolas.groupBy("CO_MUNICIPIO", "NU_ANO_CENSO")
    .agg(F.round(F.avg("infra_score"), 2).alias("avg_infra_score"))
)

(
    ideb.join(
        escolas_agg,
        (ideb.CO_MUNICIPIO == escolas_agg.CO_MUNICIPIO) &
        (ideb.ano == escolas_agg.NU_ANO_CENSO),
        "inner",
    )
    .select(
        ideb.CO_MUNICIPIO, ideb.ano, ideb.SEGMENTO, ideb.ideb,
        escolas_agg.avg_infra_score,
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_INFRA_VS_IDEB)
)
print(f"[OK] {GOLD_INFRA_VS_IDEB}")

# COMMAND ----------

# MAGIC %md ## 4. Evolução Histórica

# COMMAND ----------

REGIAO_MAP = {
    "AC":"Norte","AM":"Norte","AP":"Norte","PA":"Norte","RO":"Norte","RR":"Norte","TO":"Norte",
    "AL":"Nordeste","BA":"Nordeste","CE":"Nordeste","MA":"Nordeste","PB":"Nordeste",
    "PE":"Nordeste","PI":"Nordeste","RN":"Nordeste","SE":"Nordeste",
    "DF":"Centro-Oeste","GO":"Centro-Oeste","MS":"Centro-Oeste","MT":"Centro-Oeste",
    "ES":"Sudeste","MG":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","RS":"Sul","SC":"Sul",
}

mapping_expr = F.create_map([F.lit(x) for pair in REGIAO_MAP.items() for x in pair])

(
    enem
    .withColumn("regiao", mapping_expr[F.col("SG_UF_ESC")])
    .groupBy("NU_ANO", "regiao")
    .agg(
        F.count("*").alias("qt_candidatos"),
        F.round(F.avg("nota_media"), 2).alias("media_geral"),
        F.round(F.avg("NU_NOTA_MT"), 2).alias("media_matematica"),
        F.round(F.avg("NU_NOTA_REDACAO"), 2).alias("media_redacao"),
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_EVOLUCAO_HISTORICA)
)
print(f"[OK] {GOLD_EVOLUCAO_HISTORICA}")

# COMMAND ----------

# MAGIC %md ## 5. Escola Performance Rank

# COMMAND ----------

w_rank_escola = Window.partitionBy("NU_ANO", "TP_ESCOLA").orderBy(F.desc("media_geral"))

(
    enem.filter(F.col("CO_MUNICIPIO_ESC").isNotNull())
    .groupBy("NU_ANO", "SG_UF_ESC", "CO_MUNICIPIO_ESC", "TP_ESCOLA")
    .agg(
        F.count("*").alias("qt_candidatos"),
        F.round(F.avg("nota_media"), 2).alias("media_geral"),
        F.round(F.stddev("nota_media"), 2).alias("desvio_padrao"),
    )
    .filter(F.col("qt_candidatos") >= 10)
    .withColumn("rank_por_tipo", F.rank().over(w_rank_escola))
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("NU_ANO")
    .saveAsTable(GOLD_ESCOLA_RANK)
)
print(f"[OK] {GOLD_ESCOLA_RANK}")
