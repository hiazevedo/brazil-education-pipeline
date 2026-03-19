-- ============================================================
-- CHART: Line chart — Evolução do IDEB ao longo dos anos
-- STORY: Tendência de melhora do ensino fundamental.
--        Comparação entre Anos Iniciais (EF1) e Anos Finais (EF2)
--        e entre rede pública e privada.
-- TABLE: silver.ideb
-- ============================================================

-- Média nacional por segmento e ano
SELECT
    ano,
    SEGMENTO,
    REDE,
    COUNT(DISTINCT CO_MUNICIPIO)     AS n_municipios,
    ROUND(AVG(ideb), 2)              AS media_ideb,
    ROUND(MIN(ideb), 2)              AS min_ideb,
    ROUND(MAX(ideb), 2)              AS max_ideb
FROM education_pipeline.silver.ideb
WHERE ideb IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY SEGMENTO, REDE, ano;
