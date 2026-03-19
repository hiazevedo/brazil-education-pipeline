# Central configuration — catalog, schemas, and volume paths.
# Used by all notebooks via: from brazil_education_pipeline.config import *

CATALOG = "education_pipeline"

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"
SCHEMA_ML     = "ml_features"

VOLUME_RAW = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/raw_files"

# Table FQNs
BRONZE_ENEM           = f"{CATALOG}.{SCHEMA_BRONZE}.enem_raw"
BRONZE_CENSO_ESCOLAR  = f"{CATALOG}.{SCHEMA_BRONZE}.censo_escolar_raw"
BRONZE_IDEB           = f"{CATALOG}.{SCHEMA_BRONZE}.ideb_raw"

SILVER_ENEM           = f"{CATALOG}.{SCHEMA_SILVER}.enem"
SILVER_ESCOLAS        = f"{CATALOG}.{SCHEMA_SILVER}.escolas"
SILVER_IDEB           = f"{CATALOG}.{SCHEMA_SILVER}.ideb"

GOLD_DESEMPENHO_PERFIL    = f"{CATALOG}.{SCHEMA_GOLD}.desempenho_por_perfil"
GOLD_DESIGUALDADE_REGIONAL= f"{CATALOG}.{SCHEMA_GOLD}.desigualdade_regional"
GOLD_INFRA_VS_IDEB        = f"{CATALOG}.{SCHEMA_GOLD}.infraestrutura_vs_ideb"
GOLD_EVOLUCAO_HISTORICA   = f"{CATALOG}.{SCHEMA_GOLD}.evolucao_historica"
GOLD_ESCOLA_RANK          = f"{CATALOG}.{SCHEMA_GOLD}.escola_performance_rank"

ML_FEATURES      = f"{CATALOG}.{SCHEMA_ML}.enem_features"
ML_PREDICTIONS   = f"{CATALOG}.{SCHEMA_ML}.enem_predictions"
