-- ============================================================
-- CHART: Table + Bar chart por cluster
-- STORY: K-Means agrupa os ~5.500 municípios em 5 perfis.
--        Cada cluster conta uma história diferente sobre
--        desigualdade educacional no Brasil.
-- TABLE: ml_features.municipio_clusters
-- ============================================================

-- Perfil médio de cada cluster
SELECT
    cluster,
    COUNT(*)                                   AS n_municipios,
    ROUND(AVG(avg_nota), 2)                    AS avg_nota_enem,
    ROUND(AVG(perc_baixa_renda) * 100, 1)      AS perc_baixa_renda_pct,
    ROUND(AVG(avg_infra), 2)                   AS avg_infra_score,
    ROUND(AVG(perc_escola_publica) * 100, 1)   AS perc_escola_publica_pct,
    ROUND(AVG(vulnerabilidade), 3)             AS vulnerabilidade_media,
    -- Região dominante no cluster
    MODE(regiao)                               AS regiao_predominante
FROM education_pipeline.ml_features.municipio_clusters
GROUP BY 1
ORDER BY avg_nota_enem DESC;

-- ============================================================
-- Distribuição de clusters por região (para stacked bar)
-- ============================================================
/*
SELECT
    regiao,
    cluster,
    COUNT(*) AS n_municipios
FROM education_pipeline.ml_features.municipio_clusters
GROUP BY 1, 2
ORDER BY 1, 2;
*/
