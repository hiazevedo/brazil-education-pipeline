-- ============================================================
-- CHART: Bar chart por região + breakdown por tipo de escola
-- STORY: Alunos de baixa renda (Q006 A/B/C) acima da média
--        nacional — onde eles estão e qual o perfil?
--        Revela que o mérito individual existe, mas não é uniforme.
-- TABLE: gold.aluno_improvavel
-- ============================================================

-- Totais por região
SELECT
    regiao,
    CASE TP_ESCOLA
        WHEN 2 THEN 'Federal'
        WHEN 3 THEN 'Estadual'
        WHEN 4 THEN 'Municipal'
        WHEN 5 THEN 'Privada'
    END                              AS tipo_escola,
    CASE TP_COR_RACA
        WHEN 1 THEN 'Branca'
        WHEN 2 THEN 'Preta'
        WHEN 3 THEN 'Parda'
        WHEN 4 THEN 'Amarela'
        WHEN 5 THEN 'Indígena'
    END                              AS raca,
    SUM(qt_improvavel)               AS total_improvavel,
    ROUND(AVG(media_nota), 2)        AS media_nota,
    ROUND(AVG(media_redacao), 2)     AS media_redacao,
    ROUND(AVG(media_matematica), 2)  AS media_matematica
FROM education_pipeline.gold.aluno_improvavel
WHERE regiao IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
