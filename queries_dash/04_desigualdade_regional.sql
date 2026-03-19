-- ============================================================
-- CHART: Bar chart (ordenado) + mapa coroplético por UF
-- STORY: Ranking dos estados por nota média e gap em relação
--        à média nacional. Norte/Nordeste vs Sul/Sudeste.
-- TABLE: gold.desigualdade_regional
-- ============================================================

SELECT
    SG_UF_ESC                                                AS uf,
    qt_candidatos,
    media_geral,
    rank_uf,
    ROUND(gap_vs_media_nacional, 2)                          AS gap_vs_media_nacional,
    CASE
        WHEN gap_vs_media_nacional >= 10  THEN 'Acima (+10)'
        WHEN gap_vs_media_nacional >= 0   THEN 'Acima'
        WHEN gap_vs_media_nacional >= -10 THEN 'Abaixo'
        ELSE                                   'Abaixo (-10)'
    END                                                      AS categoria
FROM education_pipeline.gold.desigualdade_regional
WHERE SG_UF_ESC IS NOT NULL
ORDER BY rank_uf;
