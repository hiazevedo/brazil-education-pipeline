# Databricks notebook source

# MAGIC %md
# MAGIC # ML — Treinamento do Modelo
# MAGIC Treina um **RandomForestClassifier** sobre `ml_features.enem_features`.
# MAGIC
# MAGIC **Target**: `label_clf` — 1 se o aluno está acima da média nacional, caso contrário 0
# MAGIC
# MAGIC **Output**: modelo registrado no Unity Catalog + métricas MLflow incluindo
# MAGIC Feature Importance (a principal métrica narrativa deste projeto).

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, precision_recall_fscore_support, classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from pyspark.sql import functions as F
# Inline config — evita dependência do pacote instalado no Serverless
CATALOG      = "education_pipeline"
ML_FEATURES  = f"{CATALOG}.ml_features.enem_features"

# Workaround obrigatório no Databricks Free Edition Serverless
mlflow.autolog(disable=True)

EXPERIMENT_NAME = "/Users/{}/enem-model-training".format(
    spark.sql("SELECT current_user()").collect()[0][0]
)
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md ## Carregamento das features → Pandas

# COMMAND ----------

df_spark = spark.table(ML_FEATURES)
TRAIN_YEAR = df_spark.agg({"NU_ANO": "max"}).collect()[0][0]
print(f"Training year: {TRAIN_YEAR}")

df = df_spark.filter(F.col("NU_ANO") == TRAIN_YEAR).toPandas()
print(f"Rows: {len(df):,} | Positive rate: {df['label_clf'].mean():.1%}")

# COMMAND ----------

# MAGIC %md ## Features e divisão treino/teste

# COMMAND ----------

CATEGORICAL_COLS = ["SG_UF_ESC", "regiao", "TP_SEXO", "Q001", "Q002", "Q006"]
NUMERIC_COLS = [
    "TP_FAIXA_ETARIA", "TP_COR_RACA", "TP_ESCOLA",
    "TP_LOCALIZACAO", "infra_score",
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
]
FEATURE_COLS = CATEGORICAL_COLS + NUMERIC_COLS

X = df[FEATURE_COLS]
y = df["label_clf"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

# COMMAND ----------

# MAGIC %md ## Pipeline de pré-processamento

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

# MAGIC %md ## Treino e avaliação

# COMMAND ----------

with mlflow.start_run(run_name="enem-score-classifier"):
    clf_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", RandomForestClassifier(
            n_estimators=200, max_depth=10, n_jobs=-1,
            random_state=42, class_weight="balanced",
        )),
    ])
    clf_pipeline.fit(X_train, y_train)

    y_prob = clf_pipeline.predict_proba(X_test)[:, 1]
    y_pred = clf_pipeline.predict(X_test)

    auc                    = roc_auc_score(y_test, y_prob)
    prec, rec, f1, _       = precision_recall_fscore_support(y_test, y_pred, average="binary")

    mlflow.log_param("n_estimators",  200)
    mlflow.log_param("max_depth",     10)
    mlflow.log_param("class_weight",  "balanced")
    mlflow.log_metric("auc_roc",   round(float(auc),  4))
    mlflow.log_metric("precision", round(float(prec), 4))
    mlflow.log_metric("recall",    round(float(rec),  4))
    mlflow.log_metric("f1",        round(float(f1),   4))

    # ── Feature importance ──────────────────────────────────────────────────
    importances = clf_pipeline.named_steps["classifier"].feature_importances_
    feat_imp    = sorted(zip(FEATURE_COLS, importances), key=lambda x: x[1], reverse=True)

    for name, imp in feat_imp:
        mlflow.log_metric(f"feat_imp_{name}", round(float(imp), 4))

    # registered_model_name, signature e set_registered_model_alias removidos
    # não suportados no Serverless Free Edition — causam CONFIG_NOT_AVAILABLE
    mlflow.sklearn.log_model(clf_pipeline, "model")

    # ── Storytelling output ─────────────────────────────────────────────────
    print(f"\n[Classifier] AUC-ROC: {auc:.4f} | Recall: {rec:.4f} | F1: {f1:.4f}\n")
    print("─" * 58)
    print("  Fatores que mais impactam o desempenho no ENEM:")
    print("─" * 58)
    for i, (name, imp) in enumerate(feat_imp[:10], 1):
        bar = "█" * int(imp * 60)
        print(f"  {i:2d}. {name:<28} {imp * 100:5.1f}%  {bar}")
    print("─" * 58)
    print()
    print(classification_report(y_test, y_pred, target_names=["Abaixo da média", "Acima da média"]))
