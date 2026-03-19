# Databricks notebook source

# MAGIC %md
# MAGIC # ML — Model Training
# MAGIC Trains two models on `ml_features.enem_features`:
# MAGIC - **Classifier** (RandomForest): predicts if student is above national average
# MAGIC - **Regressor** (GBTRegressor): predicts total ENEM score
# MAGIC
# MAGIC Models are logged to MLflow and registered in Unity Catalog Model Registry.

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, Imputer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    RegressionEvaluator,
)
from brazil_education_pipeline.config import CATALOG, ML_FEATURES

mlflow.set_registry_uri("databricks-uc")

EXPERIMENT_NAME = f"/Users/higor_com@hotmail.com/enem-model-training"
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md ## Load features

# COMMAND ----------

df = spark.table(ML_FEATURES)

# Use last available year for training
TRAIN_YEAR = df.agg({"NU_ANO": "max"}).collect()[0][0]
df_train_raw, df_test_raw = (
    df.filter(F.col("NU_ANO") == TRAIN_YEAR)
    .randomSplit([0.8, 0.2], seed=42)
)

print(f"Training year: {TRAIN_YEAR} | Train: {df_train_raw.count():,} | Test: {df_test_raw.count():,}")

# COMMAND ----------

# MAGIC %md ## Feature pipeline (shared)

# COMMAND ----------

from pyspark.sql import functions as F

CATEGORICAL_COLS = ["SG_UF_ESC", "regiao"]
NUMERIC_COLS = [
    "TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA", "TP_ESCOLA",
    "TP_LOCALIZACAO", "Q001", "Q002", "Q006", "infra_score",
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
]

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in CATEGORICAL_COLS
]
imputer = Imputer(inputCols=NUMERIC_COLS, outputCols=[f"{c}_imp" for c in NUMERIC_COLS])
assembler = VectorAssembler(
    inputCols=[f"{c}_idx" for c in CATEGORICAL_COLS] + [f"{c}_imp" for c in NUMERIC_COLS],
    outputCol="features",
    handleInvalid="keep",
)

# COMMAND ----------

# MAGIC %md ## Train Classifier (RandomForest)

# COMMAND ----------

with mlflow.start_run(run_name="enem-score-classifier"):
    rf = RandomForestClassifier(
        labelCol="label_clf",
        featuresCol="features",
        numTrees=100,
        maxDepth=8,
        seed=42,
    )
    pipeline_clf = Pipeline(stages=indexers + [imputer, assembler, rf])
    model_clf = pipeline_clf.fit(df_train_raw)

    preds_clf = model_clf.transform(df_test_raw)
    evaluator_auc = BinaryClassificationEvaluator(labelCol="label_clf", metricName="areaUnderROC")
    auc = evaluator_auc.evaluate(preds_clf)

    mlflow.log_param("numTrees", 100)
    mlflow.log_param("maxDepth", 8)
    mlflow.log_metric("auc_roc", round(auc, 4))
    mlflow.spark.log_model(
        model_clf,
        "model",
        registered_model_name=f"{CATALOG}.ml_features.enem_score_classifier",
    )
    print(f"[Classifier] AUC-ROC: {auc:.4f}")

# COMMAND ----------

# MAGIC %md ## Train Regressor (GBT)

# COMMAND ----------

with mlflow.start_run(run_name="enem-score-regressor"):
    gbt = GBTRegressor(
        labelCol="label_reg",
        featuresCol="features",
        maxIter=50,
        maxDepth=6,
        seed=42,
    )
    pipeline_reg = Pipeline(stages=indexers + [imputer, assembler, gbt])
    model_reg = pipeline_reg.fit(df_train_raw)

    preds_reg = model_reg.transform(df_test_raw)
    evaluator_r2   = RegressionEvaluator(labelCol="label_reg", metricName="r2")
    evaluator_rmse = RegressionEvaluator(labelCol="label_reg", metricName="rmse")
    r2   = evaluator_r2.evaluate(preds_reg)
    rmse = evaluator_rmse.evaluate(preds_reg)

    mlflow.log_param("maxIter", 50)
    mlflow.log_param("maxDepth", 6)
    mlflow.log_metric("r2",   round(r2, 4))
    mlflow.log_metric("rmse", round(rmse, 2))
    mlflow.spark.log_model(
        model_reg,
        "model",
        registered_model_name=f"{CATALOG}.ml_features.enem_score_regressor",
    )
    print(f"[Regressor] R²: {r2:.4f} | RMSE: {rmse:.2f}")
