-- ============================================================
-- CHART: Bar chart (3 barras)
-- STORY: A pergunta mais poderosa do projeto:
--        "Uma escola pública bem infraestruturada chega perto
--         do desempenho de escolas privadas?"
-- TABLE: ml_features.enem_features
-- ============================================================

SELECT
    CASE
        WHEN TP_ESCOLA = 5                        THEN '3 · Privada'
        WHEN TP_ESCOLA IN (2,3,4)
         AND infra_score >= 4                     THEN '2 · Pública – Alta Infraestrutura (≥4)'
        WHEN TP_ESCOLA IN (2,3,4)                 THEN '1 · Pública – Baixa Infraestrutura (<4)'
    END                                           AS perfil_escola,
    COUNT(*)                                      AS n_candidatos,
    ROUND(AVG(label_reg), 2)                      AS media_nota,
    ROUND(AVG(NU_NOTA_MT), 2)                     AS media_matematica,
    ROUND(AVG(NU_NOTA_REDACAO), 2)                AS media_redacao
FROM education_pipeline.ml_features.enem_features
WHERE TP_ESCOLA IN (2, 3, 4, 5)
  AND label_reg IS NOT NULL
GROUP BY 1
ORDER BY 1;
