# Databricks notebook source

# MAGIC %md
# MAGIC # ML — Batch Inference
# MAGIC Loads the latest registered classifier from Unity Catalog and scores
# MAGIC the full `ml_features.enem_features` table.
# MAGIC Results are written to `ml_features.enem_predictions`.

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from brazil_education_pipeline.config import CATALOG, ML_FEATURES, ML_PREDICTIONS

mlflow.set_registry_uri("databricks-uc")
CLASSIFIER_MODEL = f"{CATALOG}.ml_features.enem_score_classifier"

# COMMAND ----------

# MAGIC %md ## Load model (latest version alias = "champion")

# COMMAND ----------

model_clf = mlflow.sklearn.load_model(f"models:/{CLASSIFIER_MODEL}@champion")

# COMMAND ----------

# MAGIC %md ## Score full feature table

# COMMAND ----------

CATEGORICAL_COLS = ["SG_UF_ESC", "regiao", "TP_SEXO", "Q001", "Q002", "Q006"]
NUMERIC_COLS = [
    "TP_FAIXA_ETARIA", "TP_COR_RACA", "TP_ESCOLA",
    "TP_LOCALIZACAO", "infra_score",
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
]
FEATURE_COLS = CATEGORICAL_COLS + NUMERIC_COLS
ID_COLS      = ["NU_ANO", "SG_UF_ESC", "TP_ESCOLA", "TP_COR_RACA", "Q006"]

df = spark.table(ML_FEATURES).select(ID_COLS + FEATURE_COLS).toPandas()

X                = df[FEATURE_COLS]
df["prob_acima_media"] = model_clf.predict_proba(X)[:, 1]
df["pred_acima_media"] = model_clf.predict(X)

# COMMAND ----------

# MAGIC %md ## Write predictions

# COMMAND ----------

(
    spark.createDataFrame(df[ID_COLS + ["prob_acima_media", "pred_acima_media"]])
    .withColumn("scored_at", F.current_timestamp())
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO")
    .saveAsTable(ML_PREDICTIONS)
)

cnt = spark.table(ML_PREDICTIONS).count()
print(f"[OK] {cnt:,} predictions written to {ML_PREDICTIONS}")
display(spark.table(ML_PREDICTIONS).limit(10))
