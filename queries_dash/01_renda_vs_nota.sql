-- ============================================================
-- CHART: Bar chart (horizontal) — Nota média por faixa de renda
-- STORY: A curva mais impactante do projeto. Mostra como a nota
--        sobe progressivamente com a renda familiar.
-- TABLE: gold.desempenho_por_perfil
-- ============================================================

SELECT
    CASE Q006
        WHEN 'A' THEN '01 · Sem renda'
        WHEN 'B' THEN '02 · Até R$ 1.320'
        WHEN 'C' THEN '03 · R$ 1.320 – 1.980'
        WHEN 'D' THEN '04 · R$ 1.980 – 2.640'
        WHEN 'E' THEN '05 · R$ 2.640 – 3.300'
        WHEN 'F' THEN '06 · R$ 3.300 – 3.960'
        WHEN 'G' THEN '07 · R$ 3.960 – 5.280'
        WHEN 'H' THEN '08 · R$ 5.280 – 6.600'
        WHEN 'I' THEN '09 · R$ 6.600 – 7.920'
        WHEN 'J' THEN '10 · R$ 7.920 – 9.240'
        WHEN 'K' THEN '11 · R$ 9.240 – 10.560'
        WHEN 'L' THEN '12 · R$ 10.560 – 13.200'
        WHEN 'M' THEN '13 · R$ 13.200 – 15.840'
        WHEN 'N' THEN '14 · R$ 15.840 – 19.800'
        WHEN 'O' THEN '15 · R$ 19.800 – 26.400'
        WHEN 'P' THEN '16 · Acima de R$ 26.400'
        ELSE 'Não declarado'
    END                              AS faixa_renda,
    SUM(qt_candidatos)               AS n_candidatos,
    ROUND(AVG(media_geral), 2)       AS media_nota,
    ROUND(AVG(media_matematica), 2)  AS media_matematica,
    ROUND(AVG(media_redacao), 2)     AS media_redacao
FROM education_pipeline.gold.desempenho_por_perfil
WHERE Q006 NOT IN ('Q')           -- remove "não declarado"
  AND Q006 IS NOT NULL
GROUP BY 1
ORDER BY 1;
