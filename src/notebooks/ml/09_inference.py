# Databricks notebook source

# MAGIC %md
# MAGIC # ML — Batch Inference
# MAGIC Loads the latest production models from Unity Catalog and scores
# MAGIC the full `ml_features.enem_features` table.
# MAGIC Results are written to `ml_features.enem_predictions`.

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from brazil_education_pipeline.config import CATALOG, ML_FEATURES, ML_PREDICTIONS

mlflow.set_registry_uri("databricks-uc")

CLASSIFIER_MODEL = f"{CATALOG}.ml_features.enem_score_classifier"
REGRESSOR_MODEL  = f"{CATALOG}.ml_features.enem_score_regressor"

# COMMAND ----------

# MAGIC %md ## Load models (latest version alias = "champion")

# COMMAND ----------

model_clf = mlflow.sklearn.load_model(f"models:/{CLASSIFIER_MODEL}@champion")
model_reg = mlflow.sklearn.load_model(f"models:/{REGRESSOR_MODEL}@champion")

# COMMAND ----------

# MAGIC %md ## Score

# COMMAND ----------

CATEGORICAL_COLS = ["SG_UF_ESC", "regiao", "TP_SEXO", "Q001", "Q002", "Q006"]
NUMERIC_COLS = [
    "TP_FAIXA_ETARIA", "TP_COR_RACA", "TP_ESCOLA",
    "TP_LOCALIZACAO", "infra_score",
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
]
FEATURE_COLS = CATEGORICAL_COLS + NUMERIC_COLS

ID_COLS = ["NU_ANO", "SG_UF_ESC", "TP_ESCOLA", "TP_COR_RACA", "Q006"]

df_spark = spark.table(ML_FEATURES)
df = df_spark.select(ID_COLS + FEATURE_COLS).toPandas()

X = df[FEATURE_COLS]
prob_acima_media = model_clf.predict_proba(X)[:, 1]
pred_acima_media = model_clf.predict(X)
pred_nota_media  = model_reg.predict(X)

df["prob_acima_media"] = prob_acima_media
df["pred_acima_media"] = pred_acima_media
df["pred_nota_media"]  = pred_nota_media

# COMMAND ----------

# MAGIC %md ## Write predictions

# COMMAND ----------

df_predictions = (
    spark.createDataFrame(df[ID_COLS + ["prob_acima_media", "pred_acima_media", "pred_nota_media"]])
    .withColumn("scored_at", F.current_timestamp())
)

(
    df_predictions.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO")
    .saveAsTable(ML_PREDICTIONS)
)

print(f"[OK] {df_predictions.count():,} predictions written to {ML_PREDICTIONS}")
display(df_predictions.limit(10))
