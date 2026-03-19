# Databricks notebook source
# MAGIC %md
# MAGIC # Desigualdade Educacional no Brasil — ENEM
# MAGIC Análise exploratória

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# Tema escuro global
plt.rcParams.update({
    "figure.facecolor": "#0d1117", "axes.facecolor":  "#161b22",
    "axes.edgecolor":   "#30363d", "axes.labelcolor": "#c9d1d9",
    "axes.titlecolor":  "#ffffff", "xtick.color":     "#8b949e",
    "ytick.color":      "#8b949e", "text.color":      "#c9d1d9",
    "grid.color":       "#21262d", "grid.linestyle":  "--",
    "grid.alpha":       0.5,       "font.family":     "monospace",
})
COLORS = ["#58a6ff","#3fb950","#ffa657","#f78166","#d2a8ff","#79c0ff","#56d364"]
C_MAIN  = "#58a6ff"
C_RED   = "#f78166"
C_GREEN = "#3fb950"
C_GRAY  = "#8b949e"
REGIONS = {"Norte": "#ffa657", "Nordeste": "#f78166", "Centro-Oeste": "#d2a8ff",
           "Sudeste": "#58a6ff", "Sul": "#3fb950"}

def hbar(ax, df, col_val, col_label, color=C_MAIN, fmt=".1f"):
    bars = ax.barh(df[col_label], df[col_val], color=color, edgecolor="#30363d")
    ax.bar_label(bars, fmt=f"%{fmt}", padding=4, fontsize=10, color="#c9d1d9")
    ax.set_xlabel("")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.5)
    ax.spines[["top","right","left"]].set_visible(False)

print("✓ Bibliotecas carregadas com tema escuro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 · Renda vs Nota
# MAGIC A curva mais impactante do projeto: nota sobe progressivamente com a renda familiar.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC A progressão é quase perfeitamente linear: cada faixa de renda acima da anterior
# MAGIC corresponde a uma nota maior. O gap entre a faixa mais baixa (sem renda) e a mais
# MAGIC alta (> R$ 26.400) costuma superar **100 pontos** — equivalente a anos de escolaridade.
# MAGIC Isso evidencia que o ENEM, embora meritocrático na forma, é um espelho das
# MAGIC desigualdades socioeconômicas do país. A curva abrupta nas faixas D→G (classe média
# MAGIC baixa) é o ponto de inflexão mais relevante para políticas públicas.

# COMMAND ----------

df_renda = spark.sql("""
    SELECT
        CASE Q006
            WHEN 'A' THEN '01 · Sem renda'
            WHEN 'B' THEN '02 · Até R$ 1.320'
            WHEN 'C' THEN '03 · R$ 1.320–1.980'
            WHEN 'D' THEN '04 · R$ 1.980–2.640'
            WHEN 'E' THEN '05 · R$ 2.640–3.300'
            WHEN 'F' THEN '06 · R$ 3.300–3.960'
            WHEN 'G' THEN '07 · R$ 3.960–5.280'
            WHEN 'H' THEN '08 · R$ 5.280–6.600'
            WHEN 'I' THEN '09 · R$ 6.600–7.920'
            WHEN 'J' THEN '10 · R$ 7.920–9.240'
            WHEN 'K' THEN '11 · R$ 9.240–10.560'
            WHEN 'L' THEN '12 · R$ 10.560–13.200'
            WHEN 'M' THEN '13 · R$ 13.200–15.840'
            WHEN 'N' THEN '14 · R$ 15.840–19.800'
            WHEN 'O' THEN '15 · R$ 19.800–26.400'
            WHEN 'P' THEN '16 · Acima de R$ 26.400'
        END AS faixa_renda,
        ROUND(AVG(media_geral), 2)      AS media_nota,
        ROUND(AVG(media_matematica), 2) AS media_matematica,
        ROUND(AVG(media_redacao), 2)    AS media_redacao,
        SUM(qt_candidatos)              AS n_candidatos
    FROM education_pipeline.gold.desempenho_por_perfil
    WHERE Q006 NOT IN ('Q') AND Q006 IS NOT NULL
    GROUP BY 1 ORDER BY 1
""").toPandas()

fig, ax = plt.subplots(figsize=(11, 7))
colors_grad = [COLORS[0]] * len(df_renda)
hbar(ax, df_renda, "media_nota", "faixa_renda", color=colors_grad)
ax.set_title("Nota Média no ENEM por Faixa de Renda Familiar", fontsize=14, fontweight="bold", pad=14)
ax.set_xlabel("Nota Média")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 · Gap Racial

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC Amarelos e Brancos ocupam consistentemente as primeiras posições; Pretos, Pardos e
# MAGIC Indígenas ficam abaixo da média. O gap entre Branca e Preta/Parda gira em torno de
# MAGIC **30–50 pontos** na nota geral. Parte desse gap é explicado pela renda (brancos têm
# MAGIC renda média maior), mas estudos mostram gap residual mesmo controlando pela renda —
# MAGIC indicando barreiras estruturais além do acesso econômico. A Redação é a disciplina
# MAGIC onde o gap racial é mais pronunciado.

# COMMAND ----------

df_racial = spark.sql("""
    SELECT
        CASE TP_COR_RACA
            WHEN 1 THEN 'Branca'   WHEN 2 THEN 'Preta'
            WHEN 3 THEN 'Parda'    WHEN 4 THEN 'Amarela'
            WHEN 5 THEN 'Indígena'
        END                             AS raca,
        ROUND(AVG(media_geral), 2)      AS media_nota,
        ROUND(AVG(media_matematica), 2) AS media_matematica,
        ROUND(AVG(media_redacao), 2)    AS media_redacao,
        SUM(qt_candidatos)              AS n_candidatos
    FROM education_pipeline.gold.desempenho_por_perfil
    WHERE TP_COR_RACA BETWEEN 1 AND 5
    GROUP BY 1 ORDER BY 2 DESC
""").toPandas()

fig, axes = plt.subplots(1, 3, figsize=(14, 4), sharey=True)
metrics = [("media_nota", "Nota Geral"), ("media_matematica", "Matemática"), ("media_redacao", "Redação")]

for ax, (col, label) in zip(axes, metrics):
    df_sorted = df_racial.sort_values(col, ascending=False)
    colors_bar = [COLORS[1], COLORS[0], COLORS[2], COLORS[3], COLORS[6]][:len(df_sorted)]
    bars = ax.barh(df_sorted["raca"], df_sorted[col], color=colors_bar, edgecolor="#30363d")
    ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=10, color="#c9d1d9")
    ax.set_title(label, fontsize=12, fontweight="bold")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.5)
    ax.spines[["top","right","left"]].set_visible(False)

fig.suptitle("Gap Racial no ENEM", fontsize=14, fontweight="bold", y=1.02)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 · Gap de Gênero por Disciplina
# MAGIC Homens lideram em Matemática e CN; mulheres em Redação e LC.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC O padrão é consistente com literatura global sobre gênero em educação: homens
# MAGIC superam mulheres em Matemática e Ciências da Natureza (raciocínio lógico-quantitativo);
# MAGIC mulheres lideram em Redação e Linguagens (competências linguísticas e comunicativas).
# MAGIC A diferença em MT costuma ser a mais pronunciada (~15–25 pontos). Esse padrão alimenta
# MAGIC escolhas de carreira — menos mulheres em STEM — e é um ponto de partida para discutir
# MAGIC estereótipos de gênero no ensino básico.

# COMMAND ----------

df_genero = spark.sql("""
    SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END AS genero,
           'CN' AS disciplina, media_cn AS media FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M','F')
    UNION ALL
    SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
           'CH', media_ch FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M','F')
    UNION ALL
    SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
           'LC', media_lc FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M','F')
    UNION ALL
    SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
           'MT', media_mt FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M','F')
    UNION ALL
    SELECT CASE TP_SEXO WHEN 'M' THEN 'Masculino' WHEN 'F' THEN 'Feminino' END,
           'Redação', media_redacao FROM education_pipeline.gold.gap_genero WHERE TP_SEXO IN ('M','F')
""").toPandas()

pivot = df_genero.pivot(index="disciplina", columns="genero", values="media").reindex(
    ["CN", "CH", "LC", "MT", "Redação"]
)

fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(pivot))
w = 0.35
b1 = ax.bar(x - w/2, pivot["Feminino"],  w, label="Feminino",  color=COLORS[3], alpha=0.9, edgecolor="#30363d")
b2 = ax.bar(x + w/2, pivot["Masculino"], w, label="Masculino", color=COLORS[0], alpha=0.9, edgecolor="#30363d")
ax.bar_label(b1, fmt="%.1f", fontsize=9, padding=3, color="#c9d1d9")
ax.bar_label(b2, fmt="%.1f", fontsize=9, padding=3, color="#c9d1d9")
ax.set_xticks(x)
ax.set_xticklabels(pivot.index)
ax.set_ylabel("Nota Média")
ax.set_title("Gap de Gênero por Disciplina", fontsize=14, fontweight="bold", pad=14)
ax.legend(framealpha=0.95, edgecolor="#30363d")
ax.grid(axis="y", alpha=0.5)
ax.spines[["top","right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 · Desigualdade Regional — Ranking por UF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC O ranking expõe a fratura Norte/Nordeste vs Sul/Sudeste. Estados como DF, SP e SC
# MAGIC ficam consistentemente no topo; AM, AP e MA no fundo. A linha tracejada da média
# MAGIC nacional divide o mapa em dois Brasis. O gap entre o primeiro e o último colocado
# MAGIC costuma passar de **50 pontos** — tamanho suficiente para fazer diferença na
# MAGIC classificação em vestibulares federais. Importante notar: parte do gap se explica
# MAGIC pela composição demográfica (renda, raça) de cada estado, não só pela qualidade
# MAGIC das escolas.

# COMMAND ----------

df_regional = spark.sql("""
    SELECT SG_UF_ESC AS uf, media_geral, rank_uf,
           ROUND(gap_vs_media_nacional, 2) AS gap,
           CASE
               WHEN gap_vs_media_nacional >= 10  THEN 'Muito acima'
               WHEN gap_vs_media_nacional >= 0   THEN 'Acima'
               WHEN gap_vs_media_nacional >= -10 THEN 'Abaixo'
               ELSE 'Muito abaixo'
           END AS categoria
    FROM education_pipeline.gold.desigualdade_regional
    WHERE SG_UF_ESC IS NOT NULL
    ORDER BY rank_uf
""").toPandas()

color_map = {"Muito acima": COLORS[1], "Acima": "#82e0aa",
             "Abaixo": "#ffa657",   "Muito abaixo": COLORS[3]}
colors = df_regional["categoria"].map(color_map)
media_nacional = df_regional["media_geral"].mean()

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(df_regional["uf"][::-1], df_regional["media_geral"][::-1],
               color=colors[::-1], edgecolor="#30363d")
ax.axvline(media_nacional, color="#8b949e", linestyle="--", linewidth=1.5,
           label=f"Média nacional ({media_nacional:.1f})")
ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=9, color="#c9d1d9")
ax.set_title("Nota Média no ENEM por Estado", fontsize=14, fontweight="bold", pad=14)
ax.set_xlabel("Nota Média")
ax.grid(axis="x", alpha=0.5)
ax.spines[["top","right","left"]].set_visible(False)

from matplotlib.patches import Patch
legend_handles = [Patch(facecolor=v, edgecolor="#30363d", label=k) for k, v in color_map.items()]
ax.legend(handles=legend_handles, loc="lower right", framealpha=0.95,
          fontsize=9, edgecolor="#30363d")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 · Escola Pública vs Privada
# MAGIC "A escola importa — mas qual escola?"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC O resultado mais revelador: **escolas federais** (CEFET, Colégio Pedro II, IFs)
# MAGIC apresentam desempenho próximo ou superior ao das privadas — mas atendem uma
# MAGIC fração minúscula dos estudantes. Estaduais e municipais ficam abaixo da privada,
# MAGIC mas o gap é menor do que o senso comum sugere. A Redação é a disciplina onde
# MAGIC a privada mais se destaca. A pergunta relevante para política pública: o que as
# MAGIC federais fazem de diferente? Infraestrutura, seleção de alunos, ou pedagogia?

# COMMAND ----------

df_escola = spark.sql("""
    SELECT
        CASE TP_ESCOLA
            WHEN 2 THEN '2 · Federal'
            WHEN 3 THEN '3 · Estadual'
            WHEN 4 THEN '4 · Municipal'
            WHEN 5 THEN '5 · Privada'
        END                                    AS perfil_escola,
        SUM(qt_candidatos)                     AS n_candidatos,
        ROUND(AVG(media_geral), 2)             AS media_nota,
        ROUND(AVG(media_matematica), 2)        AS media_matematica,
        ROUND(AVG(media_redacao), 2)           AS media_redacao
    FROM education_pipeline.gold.desempenho_por_perfil
    WHERE TP_ESCOLA IN (2,3,4,5)
    GROUP BY 1
    ORDER BY 1
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(df_escola))
w = 0.25
metrics = [("media_nota", "Nota Geral", COLORS[0]),
           ("media_matematica", "Matemática", COLORS[4]),
           ("media_redacao", "Redação", COLORS[3])]
for i, (col, label, color) in enumerate(metrics):
    bars = ax.bar(x + (i-1)*w, df_escola[col], w, label=label, color=color, alpha=0.9, edgecolor="#30363d")
    ax.bar_label(bars, fmt="%.1f", fontsize=9, padding=3, color="#c9d1d9")

ax.set_xticks(x)
ax.set_xticklabels(df_escola["perfil_escola"])
ax.set_ylabel("Nota Média")
ax.set_title("Desempenho por Tipo de Escola", fontsize=14, fontweight="bold", pad=14)
ax.legend(framealpha=0.95, edgecolor="#30363d")
ax.set_ylim(bottom=df_escola[["media_nota","media_matematica","media_redacao"]].min().min() * 0.95)
ax.grid(axis="y", alpha=0.5)
ax.spines[["top","right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06 · Evolução do IDEB (2005–2021)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC O IDEB cresceu de forma consistente de 2005 a 2019, evidenciando avanços reais
# MAGIC no ensino básico brasileiro. A rede privada mantém IDEB superior à pública em todos
# MAGIC os segmentos, mas a **distância vem diminuindo** — sinal positivo. Anos Iniciais do
# MAGIC Fundamental cresceram mais que os Finais e o Ensino Médio, que historicamente são
# MAGIC o "gargalo" do sistema. A ruptura em 2021 reflete os impactos da pandemia
# MAGIC (ensino remoto, evasão), especialmente na rede pública.

# COMMAND ----------

df_ideb = spark.sql("""
    SELECT ano, SEGMENTO, REDE,
           ROUND(AVG(ideb), 2) AS media_ideb
    FROM education_pipeline.silver.ideb
    WHERE ideb IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY SEGMENTO, REDE, ano
""").toPandas()

df_ideb["serie"] = df_ideb["SEGMENTO"] + " · " + df_ideb["REDE"]
series = df_ideb["serie"].unique()
palette = sns.color_palette("tab10", len(series))

fig, ax = plt.subplots(figsize=(12, 6))
for color, serie in zip(palette, sorted(series)):
    d = df_ideb[df_ideb["serie"] == serie]
    ax.plot(d["ano"], d["media_ideb"], marker="o", linewidth=2.5, label=serie, color=color)
    ax.annotate(f'{d["media_ideb"].iloc[-1]:.1f}', (d["ano"].iloc[-1], d["media_ideb"].iloc[-1]),
                textcoords="offset points", xytext=(6, 0), fontsize=9, color=color)

ax.set_title("Evolução do IDEB por Segmento e Rede (2005–2021)", fontsize=14, fontweight="bold")
ax.set_xlabel("Ano")
ax.set_ylabel("IDEB Médio")
ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", framealpha=0.95,
          edgecolor="#30363d", fontsize=9)
ax.grid(alpha=0.4)
ax.spines[["top","right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07 · Infraestrutura vs Desempenho
# MAGIC Cada ponto = 1 município. Tamanho proporcional ao nº de candidatos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC A linha de tendência confirma: mais infraestrutura → melhor nota. Mas a nuvem é
# MAGIC dispersa — a correlação existe mas não é determinística. Pontos **nordestinos** (laranja/vermelho)
# MAGIC tendem a se concentrar no canto inferior esquerdo (baixa infra, baixa nota). Pontos
# MAGIC **sulistas e sudestinos** (azul/verde) dominam o canto superior direito. Os **outliers positivos**
# MAGIC (baixa infra, alta nota) são os mais interessantes: municípios que superaram as
# MAGIC condições estruturais — merecem investigação qualitativa.

# COMMAND ----------

df_infra = spark.sql("""
    SELECT SG_UF_ESC AS uf, regiao, qt_candidatos,
           ROUND(avg_nota, 2)  AS avg_nota_enem,
           ROUND(avg_infra, 2) AS avg_infra_score
    FROM education_pipeline.gold.vulnerabilidade_municipal
    WHERE avg_infra IS NOT NULL AND avg_nota IS NOT NULL AND qt_candidatos >= 50
""").toPandas()

fig, ax = plt.subplots(figsize=(11, 7))
for regiao, group in df_infra.groupby("regiao"):
    size = np.sqrt(group["qt_candidatos"]) * 1.5
    ax.scatter(group["avg_infra_score"], group["avg_nota_enem"],
               s=size, alpha=0.55, label=regiao,
               color=REGIONS.get(regiao, C_GRAY), edgecolors="white", linewidth=0.4)

z = np.polyfit(df_infra["avg_infra_score"], df_infra["avg_nota_enem"], 1)
x_line = np.linspace(df_infra["avg_infra_score"].min(), df_infra["avg_infra_score"].max(), 100)
ax.plot(x_line, np.poly1d(z)(x_line), "--", color=C_GRAY, linewidth=1.5, label="Tendência")

ax.set_xlabel("Score de Infraestrutura Escolar")
ax.set_ylabel("Nota Média ENEM")
ax.set_title("Infraestrutura Escolar vs Desempenho no ENEM\n(por município, mín. 50 candidatos)",
             fontsize=13, fontweight="bold")
ax.legend(markerscale=1.2, framealpha=0.9)
ax.grid(alpha=0.35)
ax.spines[["top","right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 08 · Aluno Improvável
# MAGIC Alunos de baixa renda (Q006 A/B/C) que ficaram acima da média nacional.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC Alunos improváveis são aqueles de baixa renda (Q006 A/B/C) que, apesar das
# MAGIC desvantagens, ficaram acima da média nacional. O Sudeste tem o maior volume absoluto
# MAGIC (mais candidatos), mas o Nordeste tem relevância estratégica: é onde estão os que
# MAGIC mais precisam de suporte. Escolas **estaduais** formam a maior parcela — faz sentido,
# MAGIC pois são as mais numerosas. Mas as **federais** têm taxa de sucesso
# MAGIC desproporcionalmente alta em relação ao volume de alunos que atendem.

# COMMAND ----------

df_imp = spark.sql("""
    SELECT regiao,
        CASE TP_ESCOLA WHEN 2 THEN 'Federal' WHEN 3 THEN 'Estadual'
                       WHEN 4 THEN 'Municipal' WHEN 5 THEN 'Privada' END AS tipo_escola,
        SUM(qt_improvavel)        AS total_improvavel,
        ROUND(AVG(media_nota), 2) AS media_nota
    FROM education_pipeline.gold.aluno_improvavel
    WHERE regiao IS NOT NULL
    GROUP BY 1, 2
    ORDER BY regiao, tipo_escola
""").toPandas()

pivot_imp = df_imp.pivot_table(index="regiao", columns="tipo_escola",
                               values="total_improvavel", fill_value=0)
ordem_regioes = [r for r in ["Norte","Nordeste","Centro-Oeste","Sudeste","Sul"]
                 if r in pivot_imp.index]
pivot_imp = pivot_imp.loc[ordem_regioes]

fig, ax = plt.subplots(figsize=(11, 5))
pivot_imp.plot(kind="bar", stacked=True, ax=ax,
               color=sns.color_palette("Set2", len(pivot_imp.columns)),
               edgecolor="white", width=0.6)
ax.set_title("Alunos Improváveis por Região e Tipo de Escola", fontsize=14, fontweight="bold")
ax.set_xlabel("")
ax.set_ylabel("Total de Alunos")
ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
ax.legend(title="Tipo de escola", bbox_to_anchor=(1.01, 1), loc="upper left")
ax.grid(axis="y", alpha=0.4)
ax.spines[["top","right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 09 · Vulnerabilidade Municipal
# MAGIC Concentração de municípios de alta vulnerabilidade no Norte e Nordeste.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC Leia cada linha como o "perfil" educacional do estado. Estados com a coluna
# MAGIC **"Alta/Muito Alta" dominada por números grandes** são os mais preocupantes (MA, PA, BA).
# MAGIC Estados com a coluna **"Baixa" predominante** são os mais desenvolvidos educacionalmente
# MAGIC (SC, RS, SP). O índice de vulnerabilidade combina nota ENEM, % baixa renda e
# MAGIC infraestrutura escolar — é um indicador composto, não apenas de pobreza.
# MAGIC Estados com muitos municípios pequenos (interiorização) tendem a ter mais células
# MAGIC na coluna Alta por questão de volume.

# COMMAND ----------

df_vuln = spark.sql("""
    SELECT SG_UF_ESC AS uf, nivel_vulnerabilidade, COUNT(*) AS n_municipios
    FROM education_pipeline.gold.vulnerabilidade_municipal
    GROUP BY 1, 2
    ORDER BY uf, nivel_vulnerabilidade
""").toPandas()

pivot_vuln = df_vuln.pivot_table(index="uf", columns="nivel_vulnerabilidade",
                                  values="n_municipios", fill_value=0)
nivel_order = [c for c in ["Baixa", "Média", "Alta", "Muito Alta"] if c in pivot_vuln.columns]
pivot_vuln = pivot_vuln[nivel_order]

fig, ax = plt.subplots(figsize=(8, 12))
sns.heatmap(pivot_vuln, annot=True, fmt="d", cmap="YlOrRd",
            linewidths=0.5, linecolor="#0d1117",
            cbar_kws={"label": "Nº de Municípios"},
            annot_kws={"size": 10}, ax=ax)
ax.set_title("Distribuição de Vulnerabilidade por UF", fontsize=14, fontweight="bold", pad=14)
ax.set_xlabel("Nível de Vulnerabilidade")
ax.set_ylabel("")
ax.tick_params(axis="y", labelsize=10, rotation=0)
ax.tick_params(axis="x", labelsize=10, rotation=0)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Perfil dos Clusters (K-Means)
# MAGIC 5 grupos de municípios com perfis distintos de desigualdade educacional.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC Cada linha é um cluster (grupo de municípios similares). As cores indicam o valor
# MAGIC relativo **dentro de cada coluna** — verde = melhor, vermelho = pior.
# MAGIC O número dentro de cada célula é o **valor real** (não normalizado).
# MAGIC Leia na diagonal: clusters com nota alta tendem a ter baixa renda baixa, alta infra
# MAGIC e baixa vulnerabilidade. A exceção a esse padrão é o ponto mais interessante para
# MAGIC análise — municípios que "quebram" a correlação esperada.

# COMMAND ----------

df_clusters = spark.sql("""
    SELECT cluster, COUNT(*) AS n_municipios,
           ROUND(AVG(avg_nota), 2)                  AS avg_nota_enem,
           ROUND(AVG(perc_baixa_renda) * 100, 1)    AS perc_baixa_renda,
           ROUND(AVG(avg_infra), 2)                  AS avg_infra_score,
           ROUND(AVG(perc_escola_publica) * 100, 1)  AS perc_escola_publica,
           ROUND(AVG(vulnerabilidade), 3)             AS vulnerabilidade_media,
           MODE(regiao)                              AS regiao_predominante
    FROM education_pipeline.ml_features.municipio_clusters
    GROUP BY 1 ORDER BY avg_nota_enem DESC
""").toPandas()

def cluster_label(row):
    nota = row["avg_nota_enem"]
    regiao = row["regiao_predominante"] or "?"
    if nota >= df_clusters["avg_nota_enem"].quantile(0.75):
        perfil = "Alta performance"
    elif nota <= df_clusters["avg_nota_enem"].quantile(0.25):
        perfil = "Baixa performance"
    else:
        perfil = "Performance média"
    return f"C{int(row['cluster'])} · {perfil}\n({regiao})"

df_clusters["label"] = df_clusters.apply(cluster_label, axis=1)

metrics_heat = {
    "Nota ENEM":       "avg_nota_enem",
    "% Baixa Renda":   "perc_baixa_renda",
    "Infraestrutura":  "avg_infra_score",
    "% Esc. Pública":  "perc_escola_publica",
    "Vulnerabilidade": "vulnerabilidade_media",
}
heat_df = df_clusters.set_index("label")[list(metrics_heat.values())].copy()
heat_df.columns = list(metrics_heat.keys())
heat_norm = (heat_df - heat_df.min()) / (heat_df.max() - heat_df.min())

fig, ax = plt.subplots(figsize=(10, 5))
sns.heatmap(heat_norm, annot=heat_df.round(1), fmt="g",
            cmap="RdYlGn", linewidths=0.5, linecolor="#0d1117",
            vmin=0, vmax=1, cbar_kws={"label": "Valor normalizado (0–1)"},
            annot_kws={"size": 10}, ax=ax)
ax.set_title("Perfil dos Clusters de Municípios — K-Means (k=5)\n"
             "Cor: normalizado por coluna  |  Número: valor real",
             fontsize=13, fontweight="bold", pad=14)
ax.set_xlabel("")
ax.set_ylabel("")
ax.tick_params(axis="y", rotation=0, labelsize=9)
ax.tick_params(axis="x", rotation=15, labelsize=10)
plt.tight_layout()
plt.show()

display(df_clusters.drop(columns="label"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · Feature Importance
# MAGIC O RandomForest revela quantitativamente quais fatores determinam o desempenho.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC A Feature Importance do RandomForest mostra a contribuição relativa de cada variável
# MAGIC para a predição do desempenho. **Q006 (renda familiar)** dominando o ranking confirma
# MAGIC com evidência estatística o que os outros gráficos mostram descritivamente: o ENEM
# MAGIC é fortemente determinado pelo contexto socioeconômico. Escolaridade dos pais e tipo
# MAGIC de escola formam o segundo grupo. Variáveis como raça e localização têm peso menor,
# MAGIC mas seu efeito já está em parte capturado pela renda — use SHAP values para
# MAGIC desagregar efeitos individuais em análises futuras.

# COMMAND ----------

import mlflow

mlflow.set_tracking_uri("databricks")

# Busca o experimento pelo nome (funciona com path completo ou curto)
experiments = mlflow.search_experiments(filter_string="name LIKE '%enem-model%'")
if not experiments:
    runs = pd.DataFrame()
    print("⚠️  Experimento 'enem-model-training' não encontrado.")
else:
    runs = mlflow.search_runs(
        experiment_ids=[experiments[0].experiment_id],
        order_by=["start_time DESC"],
        max_results=1,
    )

if runs.empty:
    print("⚠️  Nenhuma run encontrada no experimento 'enem-model-training'.")
else:
    run_id = runs.iloc[0]["run_id"]
    print(f"✓ Run: {run_id}")

    feat_cols = [c for c in runs.columns if c.startswith("metrics.feat_imp_")]
    feat_imp = {
        col.replace("metrics.feat_imp_", ""): runs.iloc[0][col]
        for col in feat_cols
        if pd.notna(runs.iloc[0][col])
    }

    if not feat_imp:
        all_metrics = [c.replace("metrics.", "") for c in runs.columns
                       if c.startswith("metrics.") and pd.notna(runs.iloc[0][c])]
        print(f"⚠️  feat_imp_* não encontrado. Métricas disponíveis na run:\n{all_metrics}")
    else:
        df_fi = (pd.DataFrame(feat_imp.items(), columns=["feature", "importancia"])
                   .sort_values("importancia", ascending=True))

        colors_fi = sns.color_palette("Blues_r", len(df_fi))
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.barh(df_fi["feature"], df_fi["importancia"] * 100,
                       color=colors_fi, edgecolor="#30363d")
        ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=10, color="#c9d1d9")
        ax.set_title("Feature Importance — Fatores que Mais Determinam o Desempenho no ENEM",
                     fontsize=13, fontweight="bold", pad=14)
        ax.set_xlabel("Importância (%)")
        ax.grid(axis="x", alpha=0.4)
        ax.spines[["top","right","left"]].set_visible(False)
        plt.tight_layout()
        plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Desempenho por Faixa Etária
# MAGIC Como a idade do candidato se relaciona com o desempenho?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC O gráfico da esquerda mostra a nota; o da direita, o volume. Candidatos mais jovens
# MAGIC (17–19 anos, **verde**) tendem a ser os recém-saídos do ensino médio — maior
# MAGIC concentração e geralmente melhor preparo. A nota **cai com a idade** porque
# MAGIC candidatos mais velhos geralmente tentam o ENEM após anos fora da escola
# MAGIC (distância do conteúdo) e têm perfil socioeconômico mais vulnerável.
# MAGIC O volume altíssimo na faixa 18–19 anos confirma que o ENEM é majoritariamente
# MAGIC uma prova de conclusão do ensino médio, não de adultos em requalificação.

# COMMAND ----------

df_idade = spark.sql("""
    SELECT
        CASE TP_FAIXA_ETARIA
            WHEN 1  THEN '01 · Menor de 17'
            WHEN 2  THEN '02 · 17 anos'
            WHEN 3  THEN '03 · 18 anos'
            WHEN 4  THEN '04 · 19 anos'
            WHEN 5  THEN '05 · 20 anos'
            WHEN 6  THEN '06 · 21 a 23'
            WHEN 7  THEN '07 · 24 a 26'
            WHEN 8  THEN '08 · 27 a 30'
            WHEN 9  THEN '09 · 31 a 35'
            WHEN 10 THEN '10 · 36 a 40'
            WHEN 11 THEN '11 · 41 a 45'
            WHEN 12 THEN '12 · 46 a 50'
            WHEN 13 THEN '13 · 51 a 55'
            WHEN 14 THEN '14 · 56 a 60'
            WHEN 15 THEN '15 · 61 a 65'
            WHEN 16 THEN '16 · 66 a 70'
            WHEN 17 THEN '17 · Maior de 70'
        END                              AS faixa_etaria,
        COUNT(*)                         AS n_candidatos,
        ROUND(AVG(label_reg), 2)         AS media_nota,
        ROUND(AVG(NU_NOTA_MT), 2)        AS media_matematica,
        ROUND(AVG(NU_NOTA_REDACAO), 2)   AS media_redacao
    FROM education_pipeline.ml_features.enem_features
    WHERE TP_FAIXA_ETARIA BETWEEN 1 AND 17 AND NU_NOTA_REDACAO IS NOT NULL
    GROUP BY 1
    HAVING faixa_etaria IS NOT NULL
    ORDER BY 1
""").toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Gráfico 1: Nota média por faixa etária
jovem = {"02 · 17 anos", "03 · 18 anos", "04 · 19 anos"}
medio = {"05 · 20 anos", "06 · 21 a 23", "07 · 24 a 26"}
colors_idade = [COLORS[1] if f in jovem else COLORS[0] if f in medio else C_GRAY
                for f in df_idade["faixa_etaria"]]
hbar(ax1, df_idade, "media_nota", "faixa_etaria", color=colors_idade)
ax1.set_title("Nota Geral por Faixa Etária", fontsize=12, fontweight="bold")
ax1.set_xlabel("Nota Média")

# Gráfico 2: Volume de candidatos por faixa
bars2 = ax2.barh(df_idade["faixa_etaria"], df_idade["n_candidatos"] / 1000,
                 color=C_MAIN, alpha=0.7, edgecolor="#30363d")
ax2.bar_label(bars2, fmt="%.0fk", padding=3, fontsize=8, color="#c9d1d9")
ax2.set_title("Volume de Candidatos (mil)", fontsize=12, fontweight="bold")
ax2.set_xlabel("Candidatos (mil)")
ax2.invert_yaxis()
ax2.set_yticklabels([])
ax2.grid(axis="x", alpha=0.5)
ax2.spines[["top","right","left"]].set_visible(False)

fig.suptitle("Desempenho e Volume por Faixa Etária", fontsize=14, fontweight="bold", y=1.01)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 · Rural vs Urbano
# MAGIC A localização da escola faz diferença no desempenho?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretar
# MAGIC Escolas urbanas superam rurais em todas as regiões — o gap varia por região.
# MAGIC O gráfico da direita mostra onde a diferença urbano–rural é **maior** (vermelho)
# MAGIC e onde é **menor** (azul). Regiões com gap alto indicam que o meio rural está
# MAGIC especialmente desfavorecido — pode refletir menor acesso a professores qualificados,
# MAGIC internet, material didático. O Norte e Nordeste tendem a ter os maiores gaps,
# MAGIC agravando a dupla desvantagem: região periférica + escola rural.

# COMMAND ----------

df_localizacao = spark.sql("""
    SELECT
        CASE TP_LOCALIZACAO
            WHEN 1 THEN 'Urbana'
            WHEN 2 THEN 'Rural'
        END                              AS localizacao,
        regiao,
        COUNT(*)                         AS n_candidatos,
        ROUND(AVG(nota_media), 2)        AS media_nota,
        ROUND(AVG(NU_NOTA_MT), 2)        AS media_matematica,
        ROUND(AVG(NU_NOTA_REDACAO), 2)   AS media_redacao
    FROM education_pipeline.silver.enem
    WHERE TP_LOCALIZACAO IN (1, 2) AND regiao IS NOT NULL AND nota_media IS NOT NULL
    GROUP BY 1, 2
    HAVING localizacao IS NOT NULL
    ORDER BY regiao, localizacao
""").toPandas()

pivot_loc = df_localizacao.pivot_table(
    index="regiao", columns="localizacao", values="media_nota"
).reindex(["Norte","Nordeste","Centro-Oeste","Sudeste","Sul"])
pivot_loc["gap_urbano_rural"] = pivot_loc["Urbana"] - pivot_loc["Rural"]

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Gráfico 1: Nota por localização e região (grouped bar)
x = np.arange(len(pivot_loc))
w = 0.35
b1 = axes[0].bar(x - w/2, pivot_loc["Urbana"], w, label="Urbana", color=COLORS[0], alpha=0.9, edgecolor="#30363d")
b2 = axes[0].bar(x + w/2, pivot_loc["Rural"],  w, label="Rural",  color=COLORS[2], alpha=0.9, edgecolor="#30363d")
axes[0].bar_label(b1, fmt="%.1f", fontsize=9, padding=3, color="#c9d1d9")
axes[0].bar_label(b2, fmt="%.1f", fontsize=9, padding=3, color="#c9d1d9")
axes[0].set_xticks(x)
axes[0].set_xticklabels(pivot_loc.index, rotation=0)
axes[0].set_ylabel("Nota Média")
axes[0].set_title("Nota por Localização e Região", fontsize=12, fontweight="bold")
axes[0].legend(framealpha=0.95, edgecolor="#30363d")
axes[0].set_ylim(bottom=pivot_loc[["Urbana","Rural"]].min().min() * 0.95)
axes[0].grid(axis="y", alpha=0.5)
axes[0].spines[["top","right"]].set_visible(False)

# Gráfico 2: Gap urbano–rural por região
colors_gap = [C_RED if g > pivot_loc["gap_urbano_rural"].mean() else COLORS[0]
              for g in pivot_loc["gap_urbano_rural"]]
bars_gap = axes[1].barh(pivot_loc.index[::-1], pivot_loc["gap_urbano_rural"][::-1],
                         color=colors_gap[::-1], edgecolor="#30363d")
axes[1].bar_label(bars_gap, fmt="%.1f pts", padding=3, fontsize=10, color="#c9d1d9")
axes[1].set_title("Gap Urbano–Rural por Região", fontsize=12, fontweight="bold")
axes[1].set_xlabel("Diferença de pontos (Urbana − Rural)")
axes[1].axvline(pivot_loc["gap_urbano_rural"].mean(), color=C_GRAY,
                linestyle="--", linewidth=1.2, label="Média")
axes[1].legend(framealpha=0.9, edgecolor="#30363d", fontsize=9)
axes[1].grid(axis="x", alpha=0.5)
axes[1].spines[["top","right","left"]].set_visible(False)

fig.suptitle("Desempenho no ENEM: Escola Urbana vs Rural", fontsize=14, fontweight="bold", y=1.01)
plt.tight_layout()
plt.show()