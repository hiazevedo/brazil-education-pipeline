-- ============================================================
-- CHART: Bar chart (horizontal ordenado por nota)
-- STORY: Desigualdade racial no ENEM. Mostra o gap entre
--        brancos/amarelos e pretos/pardos/indígenas.
-- TABLE: gold.desempenho_por_perfil
-- ============================================================

SELECT
    CASE TP_COR_RACA
        WHEN 1 THEN 'Branca'
        WHEN 2 THEN 'Preta'
        WHEN 3 THEN 'Parda'
        WHEN 4 THEN 'Amarela'
        WHEN 5 THEN 'Indígena'
    END                              AS raca,
    SUM(qt_candidatos)               AS n_candidatos,
    ROUND(AVG(media_geral), 2)       AS media_nota,
    ROUND(AVG(media_matematica), 2)  AS media_matematica,
    ROUND(AVG(media_redacao), 2)     AS media_redacao
FROM education_pipeline.gold.desempenho_por_perfil
WHERE TP_COR_RACA BETWEEN 1 AND 5
GROUP BY 1
ORDER BY 3 DESC;
