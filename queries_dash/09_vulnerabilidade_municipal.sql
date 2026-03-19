-- ============================================================
-- CHART: Heatmap UF × nível de vulnerabilidade  +
--        Bar chart top-20 municípios mais vulneráveis
-- STORY: Mapa da desigualdade educacional brasileira.
--        Concentração de municípios "Alta" vulnerabilidade
--        no Norte e Nordeste.
-- TABLE: gold.vulnerabilidade_municipal
-- ============================================================

-- 1. Distribuição por UF × nível de vulnerabilidade (para heatmap)
SELECT
    SG_UF_ESC                              AS uf,
    nivel_vulnerabilidade,
    COUNT(*)                               AS n_municipios,
    ROUND(AVG(avg_nota), 2)                AS avg_nota,
    ROUND(AVG(perc_baixa_renda) * 100, 1)  AS perc_baixa_renda_pct,
    ROUND(AVG(vulnerabilidade), 3)         AS vulnerabilidade_media
FROM education_pipeline.gold.vulnerabilidade_municipal
GROUP BY 1, 2
ORDER BY uf, nivel_vulnerabilidade;

-- ============================================================
-- 2. Top 20 municípios mais vulneráveis (para tabela/bar)
-- ============================================================
/*
SELECT
    CO_MUNICIPIO,
    SG_UF_ESC                              AS uf,
    regiao,
    ROUND(vulnerabilidade, 3)              AS score_vulnerabilidade,
    nivel_vulnerabilidade,
    ROUND(avg_nota, 2)                     AS avg_nota_enem,
    ROUND(perc_baixa_renda * 100, 1)       AS perc_baixa_renda_pct,
    ROUND(avg_infra, 2)                    AS avg_infra_score,
    qt_candidatos
FROM education_pipeline.gold.vulnerabilidade_municipal
ORDER BY vulnerabilidade DESC
LIMIT 20;
*/
