-- ============================================================
-- CHART: Grouped bar — Nota por gênero × disciplina
-- STORY: Homens se saem melhor em Matemática e CN;
--        mulheres lideram em Redação e LC.
--        Útil para discutir estereótipos e escolhas de carreira.
-- TABLE: gold.gap_genero
-- ============================================================

-- Formato longo (ideal para gráficos de barras agrupadas)
SELECT
    CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END AS genero,
    'Ciências da Natureza'    AS disciplina,
    media_cn                   AS media,
    qt_candidatos
FROM education_pipeline.gold.gap_genero
WHERE TP_SEXO IN ('M', 'F')

UNION ALL
SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
       'Ciências Humanas', media_ch, qt_candidatos
FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M', 'F')

UNION ALL
SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
       'Linguagens e Códigos', media_lc, qt_candidatos
FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M', 'F')

UNION ALL
SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
       'Matemática', media_mt, qt_candidatos
FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M', 'F')

UNION ALL
SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
       'Redação', media_redacao, qt_candidatos
FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M', 'F')

ORDER BY disciplina, genero;
