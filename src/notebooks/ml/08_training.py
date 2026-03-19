# Databricks notebook source

# MAGIC %md
# MAGIC # ML — Model Training
# MAGIC Trains two models on `ml_features.enem_features` using **scikit-learn**
# MAGIC (pyspark.ml / Spark MLlib is not supported on serverless compute).
# MAGIC
# MAGIC - **Classifier** (RandomForest): predicts if student is above national average
# MAGIC - **Regressor** (GradientBoosting): predicts total ENEM score
# MAGIC
# MAGIC Models are logged to MLflow and registered in Unity Catalog Model Registry.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.metrics import roc_auc_score, r2_score, mean_squared_error
from sklearn.preprocessing import OrdinalEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from pyspark.sql import functions as F
from brazil_education_pipeline.config import CATALOG, ML_FEATURES

mlflow.set_registry_uri("databricks-uc")
EXPERIMENT_NAME = "/Users/higor_com@hotmail.com/enem-model-training"
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md ## Load features → Pandas

# COMMAND ----------

df_spark = spark.table(ML_FEATURES)
TRAIN_YEAR = df_spark.agg({"NU_ANO": "max"}).collect()[0][0]
print(f"Training year: {TRAIN_YEAR}")

df = df_spark.filter(F.col("NU_ANO") == TRAIN_YEAR).toPandas()
print(f"Rows: {len(df):,}")

# COMMAND ----------

# MAGIC %md ## Feature columns

# COMMAND ----------

CATEGORICAL_COLS = ["SG_UF_ESC", "regiao", "TP_SEXO", "Q001", "Q002", "Q006"]
NUMERIC_COLS = [
    "TP_FAIXA_ETARIA", "TP_COR_RACA", "TP_ESCOLA",
    "TP_LOCALIZACAO", "infra_score",
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
]
FEATURE_COLS = CATEGORICAL_COLS + NUMERIC_COLS

X = df[FEATURE_COLS]
y_clf = df["label_clf"]
y_reg = df["label_reg"]

X_train, X_test, y_clf_train, y_clf_test, y_reg_train, y_reg_test = (
    *__import__("sklearn.model_selection", fromlist=["train_test_split"])
    .train_test_split(X, y_clf, y_reg, test_size=0.2, random_state=42),
)

# COMMAND ----------

# MAGIC %md ## Preprocessing pipeline (shared)

# COMMAND ----------

cat_pipe = Pipeline([
    ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
    ("encoder", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
])
num_pipe = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
])
preprocessor = ColumnTransformer([
    ("cat", cat_pipe, CATEGORICAL_COLS),
    ("num", num_pipe, NUMERIC_COLS),
])

# COMMAND ----------

# MAGIC %md ## Train Classifier (RandomForest)

# COMMAND ----------

with mlflow.start_run(run_name="enem-score-classifier"):
    clf_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", RandomForestClassifier(n_estimators=100, max_depth=8, n_jobs=-1, random_state=42)),
    ])
    clf_pipeline.fit(X_train, y_clf_train)

    y_prob = clf_pipeline.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_clf_test, y_prob)

    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 8)
    mlflow.log_metric("auc_roc", round(auc, 4))
    mlflow.sklearn.log_model(
        clf_pipeline,
        "model",
        registered_model_name=f"{CATALOG}.ml_features.enem_score_classifier",
    )
    print(f"[Classifier] AUC-ROC: {auc:.4f}")

# COMMAND ----------

# MAGIC %md ## Train Regressor (GradientBoosting)

# COMMAND ----------

with mlflow.start_run(run_name="enem-score-regressor"):
    reg_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)),
    ])
    reg_pipeline.fit(X_train, y_reg_train)

    y_pred = reg_pipeline.predict(X_test)
    r2   = r2_score(y_reg_test, y_pred)
    rmse = mean_squared_error(y_reg_test, y_pred, squared=False)

    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)
    mlflow.log_metric("r2",   round(r2, 4))
    mlflow.log_metric("rmse", round(rmse, 2))
    mlflow.sklearn.log_model(
        reg_pipeline,
        "model",
        registered_model_name=f"{CATALOG}.ml_features.enem_score_regressor",
    )
    print(f"[Regressor] R²: {r2:.4f} | RMSE: {rmse:.2f}")
