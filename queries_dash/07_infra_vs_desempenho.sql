-- ============================================================
-- CHART: Scatter plot — Infraestrutura escolar vs nota ENEM
-- STORY: Municípios com melhor infraestrutura tendem a ter
--        melhores notas, mas a correlação não é perfeita —
--        outros fatores socioeconômicos pesam mais.
-- TABLE: gold.vulnerabilidade_municipal
-- ============================================================

-- Scatter: cada ponto = 1 município
SELECT
    CO_MUNICIPIO,
    SG_UF_ESC                         AS uf,
    regiao,
    qt_candidatos,
    ROUND(avg_nota, 2)                 AS avg_nota_enem,
    ROUND(avg_infra, 2)                AS avg_infra_score,
    ROUND(avg_ideb, 2)                 AS avg_ideb,
    ROUND(perc_baixa_renda * 100, 1)   AS perc_baixa_renda_pct,
    nivel_vulnerabilidade
FROM education_pipeline.gold.vulnerabilidade_municipal
WHERE avg_infra IS NOT NULL
  AND avg_nota  IS NOT NULL
  AND qt_candidatos >= 50              -- mínimo para ser representativo
ORDER BY avg_nota DESC;
