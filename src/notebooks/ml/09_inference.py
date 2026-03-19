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

model_clf = mlflow.spark.load_model(f"models:/{CLASSIFIER_MODEL}@champion")
model_reg = mlflow.spark.load_model(f"models:/{REGRESSOR_MODEL}@champion")

# COMMAND ----------

# MAGIC %md ## Score

# COMMAND ----------

df = spark.table(ML_FEATURES)

preds_clf = (
    model_clf.transform(df)
    .select("NU_ANO", "SG_UF_ESC", "TP_ESCOLA", "TP_COR_RACA", "Q006",
            F.col("probability").alias("prob_acima_media"),
            F.col("prediction").alias("pred_acima_media"))
)

preds_reg = (
    model_reg.transform(df)
    .select("NU_ANO", "SG_UF_ESC", "TP_ESCOLA", "TP_COR_RACA", "Q006",
            F.col("prediction").alias("pred_nota_media"))
)

df_predictions = (
    preds_clf
    .join(preds_reg, on=["NU_ANO", "SG_UF_ESC", "TP_ESCOLA", "TP_COR_RACA", "Q006"], how="inner")
    .withColumn("scored_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md ## Write predictions

# COMMAND ----------

(
    df_predictions.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("NU_ANO")
    .saveAsTable(ML_PREDICTIONS)
)

print(f"[OK] {df_predictions.count():,} predictions written to {ML_PREDICTIONS}")
display(df_predictions.limit(10))
