# Databricks notebook source

# MAGIC %md
# MAGIC # Gold — Analytics
# MAGIC Gera as tabelas Gold utilizadas pelo dashboard.
# MAGIC
# MAGIC | Tabela | Descrição |
# MAGIC |---|---|
# MAGIC | `gold.desempenho_por_perfil` | Nota ENEM por renda × raça × tipo de escola |
# MAGIC | `gold.desigualdade_regional` | Nota média por UF com ranking nacional e gap |
# MAGIC | `gold.infraestrutura_vs_ideb` | Infraestrutura escolar correlacionada com o IDEB |
# MAGIC | `gold.evolucao_historica` | Série temporal 2020–2023 por região |
# MAGIC | `gold.escola_performance_rank` | Ranking de escolas públicas vs privadas |

# COMMAND ----------

from pyspark.sql import functions as F, Window
CATALOG                    = "education_pipeline"
SILVER_ENEM                = f"{CATALOG}.silver.enem"
SILVER_ESCOLAS             = f"{CATALOG}.silver.escolas"
SILVER_IDEB                = f"{CATALOG}.silver.ideb"
GOLD_DESEMPENHO_PERFIL     = f"{CATALOG}.gold.desempenho_por_perfil"
GOLD_DESIGUALDADE_REGIONAL = f"{CATALOG}.gold.desigualdade_regional"
GOLD_INFRA_VS_IDEB         = f"{CATALOG}.gold.infraestrutura_vs_ideb"
GOLD_EVOLUCAO_HISTORICA    = f"{CATALOG}.gold.evolucao_historica"
GOLD_ESCOLA_RANK           = f"{CATALOG}.gold.escola_performance_rank"
GOLD_GAP_GENERO            = f"{CATALOG}.gold.gap_genero"
GOLD_ALUNO_IMPROVAVEL      = f"{CATALOG}.gold.aluno_improvavel"
GOLD_VULNERABILIDADE       = f"{CATALOG}.gold.vulnerabilidade_municipal"

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

# COMMAND ----------

# MAGIC %md ## 6. Gap de Gênero por Disciplina

# COMMAND ----------

(
    enem.groupBy("NU_ANO", "TP_SEXO")
    .agg(
        F.count("*").alias("qt_candidatos"),
        F.round(F.avg("nota_media"),        2).alias("media_geral"),
        F.round(F.avg("NU_NOTA_CN"),        2).alias("media_cn"),
        F.round(F.avg("NU_NOTA_CH"),        2).alias("media_ch"),
        F.round(F.avg("NU_NOTA_LC"),        2).alias("media_lc"),
        F.round(F.avg("NU_NOTA_MT"),        2).alias("media_mt"),
        F.round(F.avg("NU_NOTA_REDACAO"),   2).alias("media_redacao"),
    )
    .filter(F.col("TP_SEXO").isNotNull())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_GAP_GENERO)
)
print(f"[OK] {GOLD_GAP_GENERO}")

# COMMAND ----------

# MAGIC %md ## 7. Aluno Improvável
# MAGIC Alunos de baixa renda (Q006 ∈ A/B/C) com nota acima da média nacional.
# MAGIC Revela municípios e perfis onde o contexto socioeconômico foi superado.

# COMMAND ----------

media_nacional = enem.agg(F.avg("nota_media")).collect()[0][0]

(
    enem
    .withColumn("regiao", mapping_expr[F.col("SG_UF_ESC")])
    .filter(
        F.col("Q006").isin("A", "B", "C") &
        (F.col("nota_media") >= media_nacional) &
        F.col("nota_media").isNotNull()
    )
    .groupBy("NU_ANO", "SG_UF_ESC", "regiao", "TP_ESCOLA", "TP_COR_RACA")
    .agg(
        F.count("*").alias("qt_improvavel"),
        F.round(F.avg("nota_media"),      2).alias("media_nota"),
        F.round(F.avg("NU_NOTA_REDACAO"), 2).alias("media_redacao"),
        F.round(F.avg("NU_NOTA_MT"),      2).alias("media_matematica"),
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_ALUNO_IMPROVAVEL)
)
print(f"[OK] {GOLD_ALUNO_IMPROVAVEL}")
display(spark.sql(f"""
    SELECT regiao, SUM(qt_improvavel) AS total,
           ROUND(AVG(media_nota), 2) AS media
    FROM {GOLD_ALUNO_IMPROVAVEL}
    GROUP BY 1 ORDER BY 2 DESC
"""))

# COMMAND ----------

# MAGIC %md ## 8. Vulnerabilidade Municipal
# MAGIC Score composto (0–1) por município combinando:
# MAGIC - Desempenho médio no ENEM (40%)
# MAGIC - % alunos de baixa renda (35%)
# MAGIC - Infraestrutura escolar média (25%)

# COMMAND ----------

enem_mun = (
    enem
    .filter(F.col("CO_MUNICIPIO_ESC").isNotNull())
    .withColumn("regiao",        mapping_expr[F.col("SG_UF_ESC")])
    .withColumn("baixa_renda",   F.col("Q006").isin("A", "B", "C").cast("int"))
    .withColumn("escola_publica", F.col("TP_ESCOLA").isin(2, 3, 4).cast("int"))
    .groupBy("CO_MUNICIPIO_ESC", "SG_UF_ESC", "regiao")
    .agg(
        F.count("*").alias("qt_candidatos"),
        F.round(F.avg("nota_media"),                     2).alias("avg_nota"),
        F.round(F.avg(F.col("baixa_renda").cast("float")),   3).alias("perc_baixa_renda"),
        F.round(F.avg(F.col("escola_publica").cast("float")), 3).alias("perc_escola_publica"),
    )
    .filter(F.col("qt_candidatos") >= 20)
)

escolas_mun = (
    escolas.filter(F.col("NU_ANO_CENSO") == 2023)
    .groupBy("CO_MUNICIPIO")
    .agg(F.round(F.avg("infra_score"), 2).alias("avg_infra"))
)

ideb_max_ano = ideb.agg(F.max("ano")).collect()[0][0]
ideb_mun = (
    ideb.filter(F.col("ano") == ideb_max_ano)
    .groupBy("CO_MUNICIPIO")
    .agg(F.round(F.avg("ideb"), 2).alias("avg_ideb"))
)

perfil_mun = (
    enem_mun
    .join(escolas_mun, enem_mun.CO_MUNICIPIO_ESC == escolas_mun.CO_MUNICIPIO, "left")
    .join(ideb_mun,    enem_mun.CO_MUNICIPIO_ESC == ideb_mun.CO_MUNICIPIO,    "left")
    .select(
        F.col("CO_MUNICIPIO_ESC").alias("CO_MUNICIPIO"),
        "SG_UF_ESC", "regiao", "qt_candidatos",
        "avg_nota", "perc_baixa_renda", "perc_escola_publica",
        "avg_infra", "avg_ideb",
    )
)

# Min-max normalization for composite score
stats = perfil_mun.agg(
    F.min("avg_nota").alias("nota_min"), F.max("avg_nota").alias("nota_max"),
    F.min("avg_infra").alias("infra_min"), F.max("avg_infra").alias("infra_max"),
).first()

nota_range = float(stats["nota_max"] - stats["nota_min"]) or 1.0
infra_range = float(stats["infra_max"] - stats["infra_min"]) or 1.0

(
    perfil_mun
    .withColumn("nota_norm",
        (F.col("avg_nota") - F.lit(float(stats["nota_min"]))) / F.lit(nota_range))
    .withColumn("infra_norm",
        F.coalesce(
            (F.col("avg_infra") - F.lit(float(stats["infra_min"]))) / F.lit(infra_range),
            F.lit(0.5)
        ))
    .withColumn("vulnerabilidade", F.round(
        (F.lit(1.0) - F.col("nota_norm"))                          * 0.40 +
        F.coalesce(F.col("perc_baixa_renda"), F.lit(0.5))         * 0.35 +
        (F.lit(1.0) - F.col("infra_norm"))                         * 0.25,
        4
    ))
    .withColumn("nivel_vulnerabilidade",
        F.when(F.col("vulnerabilidade") >= 0.8, "Alta")
        .when(F.col("vulnerabilidade") >= 0.6, "Médio-alta")
        .when(F.col("vulnerabilidade") >= 0.4, "Média")
        .when(F.col("vulnerabilidade") >= 0.2, "Médio-baixa")
        .otherwise("Baixa"))
    .drop("nota_norm", "infra_norm")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_VULNERABILIDADE)
)
print(f"[OK] {GOLD_VULNERABILIDADE}")
display(spark.sql(f"""
    SELECT nivel_vulnerabilidade, regiao, COUNT(*) AS n_municipios,
           ROUND(AVG(avg_nota), 2) AS avg_nota,
           ROUND(AVG(perc_baixa_renda), 3) AS perc_baixa_renda
    FROM {GOLD_VULNERABILIDADE}
    GROUP BY 1, 2 ORDER BY 1, 2
"""))
