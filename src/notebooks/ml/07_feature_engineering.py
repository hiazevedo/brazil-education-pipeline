# Databricks notebook source

# MAGIC %md
# MAGIC # ML — Feature Engineering
# MAGIC Constrói `ml_features.enem_features` a partir das tabelas Silver.
# MAGIC
# MAGIC **Features (~25):**
# MAGIC - Socioeconômicas: faixa de renda (Q006), escolaridade dos pais (Q001, Q002), raça, faixa etária
# MAGIC - Geográficas: UF, região (Norte/Nordeste/etc.)
# MAGIC - Escolares: tipo (público/privado), localização (urbano/rural), infra_score
# MAGIC - Acadêmicas: notas individuais por área
# MAGIC
# MAGIC **Targets:**
# MAGIC - `label_clf`: 1 se nota_media >= média nacional, caso contrário 0
# MAGIC - `label_reg`: nota_media (contínua)

# COMMAND ----------

from pyspark.sql import functions as F
from brazil_education_pipeline.config import SILVER_ENEM, SILVER_ESCOLAS, ML_FEATURES

# COMMAND ----------

# MAGIC %md ## Carregamento das tabelas Silver

# COMMAND ----------

enem    = spark.table(SILVER_ENEM)
escolas = spark.table(SILVER_ESCOLAS)

# COMMAND ----------

# MAGIC %md ## Join do ENEM com infraestrutura escolar

# COMMAND ----------

escolas_latest = (
    escolas
    .filter(F.col("NU_ANO_CENSO") == F.lit(2023))
    .select("CO_ENTIDADE", "infra_score", "TP_LOCALIZACAO", "QT_MAT_BAS")
)

df = enem.join(escolas_latest, enem.CO_MUNICIPIO_ESC == escolas_latest.CO_ENTIDADE, "left")

# COMMAND ----------

# MAGIC %md ## Mapeamento de regiões

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

df = df.withColumn("regiao", mapping_expr[F.col("SG_UF_ESC")])

# COMMAND ----------

# MAGIC %md ## Cálculo dos targets

# COMMAND ----------

media_nacional = df.agg(F.avg("nota_media")).collect()[0][0]

df_features = (
    df
    .withColumn("label_clf", (F.col("nota_media") >= F.lit(media_nacional)).cast("int"))
    .withColumn("label_reg",  F.col("nota_media"))
    .fillna({"infra_score": 0, "TP_LOCALIZACAO": 1, "Q006": 0, "Q001": 0, "Q002": 0})
    .select(
        "NU_ANO", "SG_UF_ESC", "regiao",
        "TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA", "TP_ESCOLA",
        "TP_LOCALIZACAO", "Q001", "Q002", "Q006",
        "infra_score",
        "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
        "label_clf", "label_reg",
    )
    .filter(F.col("label_reg").isNotNull())
)

print(f"Feature rows: {df_features.count():,} | Positive class rate: {df_features.filter('label_clf=1').count() / df_features.count():.1%}")

# COMMAND ----------

# MAGIC %md ## Salvamento da tabela de features

# COMMAND ----------

(
    df_features.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO")
    .saveAsTable(ML_FEATURES)
)

print(f"[OK] Written to {ML_FEATURES}")
display(df_features.limit(5))
