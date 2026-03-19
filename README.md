# Brazil Education Pipeline

> Pipeline end-to-end de Engenharia de Dados e Machine Learning sobre desigualdade educacional brasileira com ENEM, Censo Escolar e IDEB no Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-0194E2?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Sobre o projeto

Pipeline completo de Engenharia de Dados e Machine Learning que cruza três fontes de dados públicos brasileiros — **ENEM** (microdados 2020–2023), **Censo Escolar** e **IDEB** — para revelar os fatores que mais impactam o desempenho educacional no Brasil. O pipeline cobre desde a ingestão automática via GitHub Actions até o treinamento de modelos de classificação e clustering de municípios, com 8 tabelas Gold analíticas e 13 visualizações interativas, tudo dentro do ecossistema Databricks com Unity Catalog.

---

## Arquitetura do pipeline

```
[GitHub Actions — ENEM / Censo Escolar / IDEB]
  Ingestão anual automática → Volume UC (raw_files)
           │
           ▼
┌──────────────────────────────────────┐
│              BRONZE                  │
│  enem_raw · censo_escolar_raw · ideb_raw  │
│  Delta Lake — schema inferido        │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│              SILVER                  │
│  enem · escolas · ideb               │
│  Limpeza · Tipagem · infra_score     │
│  Remoção de treineiros e nulos       │
│  nota_media calculada (5 provas)     │
└──────────────────┬───────────────────┘
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
┌──────────────────┐  ┌──────────────────────┐
│      GOLD        │  │    ML FEATURES       │
│  8 tabelas       │  │  enem_features       │
│  analíticas      │  │  ~25 features        │
└────────┬─────────┘  └──────────┬───────────┘
         │                       │
         ▼                       ▼
┌──────────────────┐  ┌──────────────────────┐
│   K-Means        │  │  RandomForest        │
│   5 clusters     │  │  Classificador       │
│   de municípios  │  │  AUC-ROC + F1        │
└──────────────────┘  └──────────┬───────────┘
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │  Batch Inference     │
                       │  enem_predictions    │
                       └──────────────────────┘
```

---

## Fontes de dados

| Fonte | Descrição | Granularidade | Período |
|-------|-----------|---------------|---------|
| **ENEM** (INEP) | Microdados de desempenho por candidato | Por candidato | 2020–2023 |
| **Censo Escolar** (INEP) | Infraestrutura e matrículas de escolas | Por escola | 2020–2023 |
| **IDEB** (INEP) | Índice de Desenvolvimento da Educação Básica | Por município | 2005–2021 |

---

## Tabelas Gold

| Tabela | Descrição |
|--------|-----------|
| `gold.desempenho_por_perfil` | Nota ENEM por renda × raça × tipo de escola |
| `gold.desigualdade_regional` | Nota média por UF com ranking nacional e gap |
| `gold.infraestrutura_vs_ideb` | Infraestrutura escolar correlacionada com o IDEB |
| `gold.evolucao_historica` | Série temporal 2020–2023 por região |
| `gold.escola_performance_rank` | Ranking de escolas públicas vs privadas |
| `gold.gap_genero` | Desempenho por disciplina separado por sexo |
| `gold.aluno_improvavel` | Alunos de baixa renda acima da média nacional |
| `gold.vulnerabilidade_municipal` | Score composto de vulnerabilidade por município (0–1) |

---

## Modelo de Machine Learning

### Classificador — Acima ou abaixo da média nacional?

| Problema | Tipo | Target | Métrica principal |
|----------|------|--------|-------------------|
| Prever se o aluno ficará acima da média | Classificação binária | `label_clf` | AUC-ROC / F1 |

### Clustering — Perfis de municípios

| Problema | Tipo | Features | Output |
|----------|------|----------|--------|
| Agrupar municípios por perfil educacional | K-Means (k=5) | avg_nota, perc_baixa_renda, avg_infra, perc_escola_publica | `ml_features.municipio_clusters` |

---

## Feature Engineering

| Grupo | Features | Fonte |
|-------|----------|-------|
| **Socioeconômicas** | Q006 (renda), Q001/Q002 (escolaridade dos pais), TP_COR_RACA | ENEM |
| **Geográficas** | SG_UF_ESC, regiao (Norte/Nordeste/etc.) | ENEM |
| **Escolares** | TP_ESCOLA (público/privado), TP_LOCALIZACAO (urbano/rural), infra_score | ENEM + Censo Escolar |
| **Demográficas** | TP_FAIXA_ETARIA, TP_SEXO | ENEM |
| **Acadêmicas** | NU_NOTA_CN, NU_NOTA_CH, NU_NOTA_LC, NU_NOTA_MT, NU_NOTA_REDACAO | ENEM |

`infra_score` (0–5): soma de 5 indicadores binários de infraestrutura — energia elétrica, água potável, biblioteca, laboratório de informática e internet.

---

## Ingestão automática — GitHub Actions

Três workflows automatizam o download e upload dos arquivos brutos para o Volume UC:

| Workflow | Trigger | Fonte |
|----------|---------|-------|
| `ingest_enem.yml` | 1º de março (anual) + manual | Portal INEP — microdados ENEM |
| `ingest_censo.yml` | Manual | Portal INEP — Censo Escolar |
| `ingest_ideb.yml` | Manual | Portal INEP — IDEB municipal |

Cada workflow faz download, filtra as colunas necessárias e faz upload para `/Volumes/education_pipeline/bronze/raw_files/`.

---

## Visualizações

| # | Gráfico | Tipo |
|---|---------|------|
| 01 | Nota média por faixa de renda | Bar chart |
| 02 | Nota média por raça | Bar chart |
| 03 | Nota média por tipo de escola (público vs privado) | Bar chart |
| 04 | Desigualdade regional por UF | Bar chart horizontal |
| 05 | Desempenho por perfil socioeconômico | Grouped bar |
| 06 | Evolução do IDEB por segmento (2005–2021) | Line chart |
| 07 | Aluno improvável — municípios de baixa renda com alta nota | Bar chart |
| 08 | Vulnerabilidade municipal por estado | Bar chart horizontal |
| 09 | Perfil dos clusters de municípios | Heatmap normalizado |
| 10 | Feature Importance do classificador | Bar chart horizontal |
| 11 | Gap de gênero por disciplina | Grouped bar |
| 12 | Nota média por faixa etária | Bar chart |
| 13 | Escola pública urbana vs rural | Bar chart |

---

## Estrutura do projeto

```
brazil-education-pipeline/
├── databricks.yml                        # Databricks Asset Bundle — Job completo
├── resources/
│   └── education_pipeline.job.yml        # Definição das tasks e dependências
├── src/
│   ├── brazil_education_pipeline/
│   │   └── config.py                     # Constantes centralizadas (nomes de tabelas)
│   └── notebooks/
│       ├── 00_setup.py                   # Cria catalog, schemas e volume
│       ├── bronze/
│       │   ├── 01_enem.py                # Ingestão ENEM → bronze.enem_raw
│       │   ├── 02_censo_escolar.py       # Ingestão Censo → bronze.censo_escolar_raw
│       │   └── 03_ideb.py                # Ingestão IDEB → bronze.ideb_raw
│       ├── silver/
│       │   ├── 04_enem.py                # Limpeza + nota_media → silver.enem
│       │   └── 05_censo_ideb.py          # infra_score + pivot IDEB → silver.escolas/ideb
│       ├── gold/
│       │   └── 06_analytics.py           # 8 tabelas Gold analíticas
│       ├── ml/
│       │   ├── 07_feature_engineering.py # ~25 features → ml_features.enem_features
│       │   ├── 08_training.py            # RandomForestClassifier + MLflow
│       │   ├── 08b_clustering.py         # K-Means municipios (k=5)
│       │   └── 09_inference.py           # Batch inference → ml_features.enem_predictions
│       └── exploratory/
│           └── dashboard_queries.py      # 13 gráficos Matplotlib/Seaborn
├── ingestion/
│   ├── enem.py                           # Script de download/upload ENEM
│   ├── censo_escolar.py                  # Script de download/upload Censo
│   └── uploader.py                       # Helper para upload ao Volume UC
└── .github/workflows/
    ├── ingest_enem.yml                   # GitHub Actions — ENEM anual
    ├── ingest_censo.yml                  # GitHub Actions — Censo Escolar
    └── ingest_ideb.yml                   # GitHub Actions — IDEB
```

---

## Databricks Job

O pipeline completo está configurado como **Databricks Job** via Asset Bundle (`databricks.yml`), com execução paralela nas camadas Bronze e Silver:

```
bronze_enem ─┐
bronze_censo ─┼─► silver_enem ──────────────────────────────────┐
bronze_ideb ─┘    silver_censo_ideb ──┬──► gold_analytics ──────┼──► ml_clustering
                                      │         │                │
                                      └──► ml_feature_eng ──────┤
                                                │                │
                                           ml_training ──────────┴──► ml_inference
```

Para fazer o deploy:

```bash
databricks bundle deploy
databricks bundle run brazil-education-pipeline
```

---

## MLflow Registry

```
education_pipeline.ml_features.enem_score_classifier
└── champion  ✅  (RandomForestClassifier — n_estimators=200, max_depth=10)
```

O modelo é registrado no Unity Catalog e pode ser carregado diretamente pelo alias:

```python
model = mlflow.sklearn.load_model("models:/education_pipeline.ml_features.enem_score_classifier@champion")
```

---

## Stack técnica

| Tecnologia | Uso |
|------------|-----|
| **Databricks Free Edition** | Ambiente Serverless AWS |
| **Unity Catalog** | Governança de dados + Model Registry |
| **MLflow** | Experiment tracking + Model Registry |
| **PySpark** | Processamento distribuído das tabelas Silver/Gold |
| **Scikit-learn** | RandomForestClassifier + KMeans + Pipeline |
| **Delta Lake** | Armazenamento ACID em todas as camadas |
| **Databricks Asset Bundles** | Orquestração do pipeline como código |
| **GitHub Actions** | Ingestão automática anual dos dados INEP |
| **Matplotlib / Seaborn** | 13 visualizações com tema dark |

---

## Como reproduzir

### Pré-requisitos
- Conta no [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Unity Catalog habilitado
- Databricks CLI instalado e configurado
- Secrets no GitHub: `DATABRICKS_HOST` e `DATABRICKS_TOKEN`

### Passo a passo

```bash
# 1. Clone o repositório
git clone https://github.com/hiazevedo/brazil-education-pipeline.git
cd brazil-education-pipeline

# 2. Configure o ambiente local
uv sync --dev

# 3. Execute o setup do Unity Catalog
databricks bundle deploy
# Rode o notebook 00_setup.py manualmente uma vez

# 4. Dispare a ingestão dos dados (via GitHub Actions ou manualmente)
# GitHub Actions → ingest_enem.yml (anos desejados, ex: 2022,2023)
# GitHub Actions → ingest_censo.yml
# GitHub Actions → ingest_ideb.yml

# 5. Execute o job completo
databricks bundle run brazil-education-pipeline
```

Ou execute os notebooks manualmente na ordem:

```
00_setup.py                   # Cria catalog, schemas e volume (executar 1x)
bronze/01_enem.py             # Ingere microdados ENEM
bronze/02_censo_escolar.py    # Ingere Censo Escolar
bronze/03_ideb.py             # Ingere IDEB
silver/04_enem.py             # Limpa e calcula nota_media
silver/05_censo_ideb.py       # Cria infra_score e pivota IDEB
gold/06_analytics.py          # Gera as 8 tabelas Gold
ml/07_feature_engineering.py  # Cria feature store
ml/08_training.py             # Treina RandomForest e registra no MLflow
ml/08b_clustering.py          # Executa K-Means de municípios
ml/09_inference.py            # Inferência em batch
exploratory/dashboard_queries.py  # Gera os 13 gráficos
```

### Unity Catalog

```
Catalog : education_pipeline
Schemas : bronze | silver | gold | ml_features
Volume  : /Volumes/education_pipeline/bronze/raw_files
```

---

## Decisões técnicas

**Por que GitHub Actions e não ingestão dentro do Databricks?**
Os arquivos do INEP chegam como ZIPs de até 2 GB hospedados no portal do governo. Fazer o download direto de um notebook Databricks depende de conectividade de saída e pode ser instável. O GitHub Actions resolve isso no lado do CI: faz o download, descompacta, filtra as colunas necessárias e faz upload para o Volume UC via Databricks CLI, desacoplando a ingestão do pipeline de transformação.

**Por que `infra_score` como soma binária (0–5) e não pesos?**
Os indicadores do Censo Escolar (energia, água, biblioteca, laboratório, internet) não têm escala comparável entre si. A soma binária é simples, interpretável e resistente a outliers — uma escola sem internet mas com biblioteca não deve ser penalizada duas vezes. O score composto de vulnerabilidade municipal aplica pesos diferentes (nota 40%, renda 35%, infra 25%) para refletir impacto real.

**Por que `label_clf` e não regressão direta da nota?**
O objetivo analítico do projeto é identificar os **fatores que separam alunos acima e abaixo da média nacional** — uma pergunta de classificação. A Feature Importance do RandomForest sobre esse binário é mais interpretável para uma audiência não técnica do que coeficientes de um modelo de regressão, servindo como o principal artefato narrativo do projeto.

---

## Portfólio

Este projeto faz parte do [Databricks Data Engineering Portfolio](https://github.com/hiazevedo/databricks-portfolio), uma série de projetos práticos cobrindo o ciclo completo de engenharia de dados com Databricks.

| # | Projeto | Tema |
|---|---------|------|
| 1 | [fuel-price-pipeline-br](https://github.com/hiazevedo/fuel-price-pipeline-br) | Batch · Medallion · ANP |
| 2 | [earthquake-streaming-pipeline](https://github.com/hiazevedo/earthquake-streaming-pipeline) | Streaming · Auto Loader · USGS |
| 3 | [earthquake-ml-pipeline](https://github.com/hiazevedo/earthquake-ml-pipeline) | ML · MLflow · Spark ML |
| 4 | [weather-dlt-pipeline](https://github.com/hiazevedo/weather-dlt-pipeline) | DLT · Workflows · Open-Meteo |
| 5 | [weather-ml-rain-forecast](https://github.com/hiazevedo/weather-ml-rain-forecast) | ML Avançado · Previsão de Chuva |
| 6 | **brazil-education-pipeline** ← você está aqui | Educação · ENEM · ML · GitHub Actions |
