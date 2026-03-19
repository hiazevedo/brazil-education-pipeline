-- ============================================================
-- CHART: Horizontal bar chart (ordenado)
-- STORY: O modelo de ML revela quantitativamente quais fatores
--        mais determinam o desempenho. Confirma o storytelling
--        socioeconômico com evidência estatística.
-- TABLE: ml_features.enem_features (via MLflow metrics)
-- NOTE:  Os valores abaixo são exemplos — substitua pelos
--        valores reais do MLflow após o treino.
--        No Databricks: Experiments > enem-model-training >
--        última run > Metrics (feat_imp_*)
-- ============================================================

-- Feature importance registrada como métricas MLflow
-- Cole os valores do MLflow aqui para montar o gráfico:

SELECT feature, importancia FROM (
    VALUES
        ('Q006 (renda familiar)',          NULL),   -- substitua pelos valores reais
        ('Q002 (escolaridade da mãe)',     NULL),
        ('Q001 (escolaridade do pai)',     NULL),
        ('TP_ESCOLA (público/privado)',    NULL),
        ('TP_COR_RACA (raça/cor)',         NULL),
        ('SG_UF_ESC (estado)',             NULL),
        ('regiao',                         NULL),
        ('infra_score',                    NULL),
        ('TP_LOCALIZACAO (rural/urbano)',  NULL),
        ('TP_FAIXA_ETARIA (idade)',        NULL)
) AS t(feature, importancia)
ORDER BY importancia DESC;

-- ============================================================
-- Alternativa: ler diretamente de mlflow.metric_history
-- (se tiver integração com mlflow via SQL no Databricks)
-- ============================================================
/*
SELECT
    REPLACE(key, 'feat_imp_', '')  AS feature,
    ROUND(value * 100, 2)          AS importancia_pct
FROM mlflow.default.metric_history
WHERE run_id = '<seu_run_id>'
  AND key LIKE 'feat_imp_%'
ORDER BY value DESC
LIMIT 16;
*/
