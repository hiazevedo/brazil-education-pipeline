# Databricks notebook source

# MAGIC %md
# MAGIC # ML — Clustering de Municípios
# MAGIC Agrupa os ~5.500 municípios brasileiros em **5 perfis educacionais**
# MAGIC usando K-Means sobre dados de desempenho, renda e infraestrutura.
# MAGIC
# MAGIC **Features**: nota média ENEM, % baixa renda, infra_score, % escola pública
# MAGIC
# MAGIC **Output**: `ml_features.municipio_clusters` — cada município com seu cluster
# MAGIC e as métricas que definem seu perfil.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
CATALOG               = "education_pipeline"
GOLD_VULNERABILIDADE  = f"{CATALOG}.gold.vulnerabilidade_municipal"
ML_MUNICIPIO_CLUSTERS = f"{CATALOG}.ml_features.municipio_clusters"

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Users/higor_com@hotmail.com/municipio-clustering")

# COMMAND ----------

# MAGIC %md ## Carregamento dos perfis municipais

# COMMAND ----------

df = spark.table(GOLD_VULNERABILIDADE).toPandas()
print(f"Municípios carregados: {len(df):,}")

CLUSTER_FEATURES = ["avg_nota", "perc_baixa_renda", "avg_infra", "perc_escola_publica"]
X = df[CLUSTER_FEATURES]

# COMMAND ----------

# MAGIC %md ## K-Means (k=5)

# COMMAND ----------

N_CLUSTERS = 5

pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler",  StandardScaler()),
    ("kmeans",  KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)),
])

with mlflow.start_run(run_name="municipio-kmeans"):
    pipeline.fit(X)
    df["cluster"] = pipeline.named_steps["kmeans"].labels_

    inertia = pipeline.named_steps["kmeans"].inertia_
    mlflow.log_param("n_clusters",    N_CLUSTERS)
    mlflow.log_param("features",      ",".join(CLUSTER_FEATURES))
    mlflow.log_metric("inertia",      inertia)
    mlflow.sklearn.log_model(pipeline, "model")

    # Cluster centroids (back to original scale)
    centroids = pd.DataFrame(
        pipeline.named_steps["scaler"].inverse_transform(
            pipeline.named_steps["kmeans"].cluster_centers_
        ),
        columns=CLUSTER_FEATURES,
    )
    centroids.index.name = "cluster"

    # Summary with dominant region per cluster
    region_mode = (
        df.groupby("cluster")["regiao"]
        .agg(lambda s: s.value_counts().index[0])
        .rename("regiao_dominante")
    )
    summary = (
        df.groupby("cluster")[CLUSTER_FEATURES + ["vulnerabilidade"]]
        .mean()
        .join(df.groupby("cluster").size().rename("n_municipios"))
        .join(region_mode)
        .round(3)
    )

    print(f"\nInertia: {inertia:,.0f}")
    print(f"\n{'─' * 75}")
    print(f"  Perfis de Municípios — K-Means k={N_CLUSTERS}")
    print(f"{'─' * 75}")
    print(summary.to_string())
    print(f"{'─' * 75}")

# COMMAND ----------

# MAGIC %md ## Salvamento dos clusters

# COMMAND ----------

result_cols = ["CO_MUNICIPIO", "SG_UF_ESC", "regiao", "cluster"] + CLUSTER_FEATURES + ["vulnerabilidade", "nivel_vulnerabilidade"]
available   = [c for c in result_cols if c in df.columns]

(
    spark.createDataFrame(df[available])
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ML_MUNICIPIO_CLUSTERS)
)
print(f"[OK] Written to {ML_MUNICIPIO_CLUSTERS}")

display(spark.sql(f"""
    SELECT cluster, regiao, COUNT(*) AS n_municipios,
           ROUND(AVG(avg_nota), 2)            AS avg_nota,
           ROUND(AVG(perc_baixa_renda), 3)    AS perc_baixa_renda,
           ROUND(AVG(vulnerabilidade), 3)      AS vulnerabilidade
    FROM {ML_MUNICIPIO_CLUSTERS}
    GROUP BY cluster, regiao
    ORDER BY cluster, regiao
"""))
