# -*- coding: utf-8 -*-
import os
import warnings
import pandas as pd
import polars as pl
from tqdm import tqdm

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ==========================================================
# CONFIG
# ==========================================================
PASTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Nova pasta\Entrega Realizada"
ARQUIVO_SAIDA = os.path.join(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Nova pasta",
    "resumo_entregadores.xlsx"
)

COL_MOTORISTA = "Entregador"
COL_REMESSA = "Remessa"
COL_BASE = "Base de entrega"

# Algumas planilhas podem ter ? normal ou o caractere cheio "？"
COL_PRAZO_CANDIDATAS = ["Entregue no prazo？", "Entregue no prazo?"]

dfs = []

# ==========================================================
# LEITURA DAS PLANILHAS
# ==========================================================
print("\n🔍 Lendo planilhas...\n")

if not os.path.isdir(PASTA):
    print(f"❌ Pasta não encontrada: {PASTA}")
    raise SystemExit

arquivos = [f for f in os.listdir(PASTA) if f.endswith((".xlsx", ".xls"))]

if not arquivos:
    print("❌ Nenhum arquivo Excel encontrado.")
    raise SystemExit

for arquivo in tqdm(arquivos):
    caminho = os.path.join(PASTA, arquivo)

    try:
        # Lê todas as abas de uma vez
        abas = pd.read_excel(caminho, sheet_name=None)

        for _, dfp in abas.items():
            if dfp is None or dfp.shape[0] == 0:
                continue
            dfs.append(dfp)

    except Exception as e:
        print(f"⚠️ Erro lendo {arquivo}: {e}")

if not dfs:
    print("❌ Nenhum dado encontrado.")
    raise SystemExit

print("\n📌 Consolidando...\n")

df_total_pd = pd.concat(dfs, ignore_index=True)

# ==========================================================
# CONVERSÃO PARA POLARS
# ==========================================================
df = pl.from_pandas(df_total_pd)

# ==========================================================
# DETECTAR COLUNA DE PRAZO
# ==========================================================
col_prazo = next((c for c in COL_PRAZO_CANDIDATAS if c in df.columns), None)

if not col_prazo:
    print("❌ Coluna 'Entregue no prazo？/?' não encontrada.")
    print("Colunas disponíveis:", df.columns)
    raise SystemExit

# ==========================================================
# VERIFICAR COLUNAS
# ==========================================================
for col in [COL_MOTORISTA, COL_REMESSA, COL_BASE, col_prazo]:
    if col not in df.columns:
        print(f"❌ Coluna '{col}' não encontrada.")
        print("Colunas disponíveis:", df.columns)
        raise SystemExit

# ==========================================================
# LIMPEZA E NORMALIZAÇÃO
# ==========================================================
df = df.with_columns([
    pl.col(COL_MOTORISTA).cast(pl.Utf8).alias(COL_MOTORISTA),
    pl.col(COL_REMESSA).cast(pl.Utf8).alias(COL_REMESSA),
    pl.col(COL_BASE).cast(pl.Utf8).alias(COL_BASE),
    pl.col(col_prazo).cast(pl.Utf8).alias(col_prazo),
])

df = df.with_columns([
    pl.col(COL_REMESSA).str.strip_chars().alias("_remessa_raw"),
    pl.col(COL_BASE).str.strip_chars().alias("_base_raw"),
    pl.col(col_prazo).str.to_lowercase().str.strip_chars().alias("_prazo_norm"),
])

# limpa remessas inválidas
df = df.with_columns([
    pl.when(
        pl.col("_remessa_raw").is_null() |
        pl.col("_remessa_raw").is_in(["", "nan", "none", "null"])
    )
    .then(None)
    .otherwise(pl.col("_remessa_raw"))
    .alias("_remessa_clean")
])

# limpa base inválida
df = df.with_columns([
    pl.when(
        pl.col("_base_raw").is_null() |
        pl.col("_base_raw").is_in(["", "nan", "none", "null"])
    )
    .then(None)
    .otherwise(pl.col("_base_raw"))
    .alias("_base_clean")
])

# ==========================================================
# AGRUPAMENTO + CONTAS
# Regras:
# - "y" = entregue
# - "n" e "" (vazio) = não tem entrega
# Contagem alinhada à existência de remessa válida.
# ==========================================================
entregue_expr = (
    (pl.col("_remessa_clean").is_not_null()) &
    (pl.col("_prazo_norm") == "y")
).cast(pl.Int64)

nao_entregue_expr = (
    (pl.col("_remessa_clean").is_not_null()) &
    (pl.col("_prazo_norm") != "y")
).cast(pl.Int64)

# ==========================================================
# RESUMO POR BASE + ENTREGADOR
# ==========================================================
resumo_base_motorista = (
    df
    .filter(pl.col("_base_clean").is_not_null())
    .group_by([COL_BASE, COL_MOTORISTA])
    .agg([
        pl.count("_remessa_clean").alias("QuantidadeRemessas"),
        entregue_expr.sum().alias("QtdEntregue"),
        nao_entregue_expr.sum().alias("QtdNaoEntregue"),
    ])
    .with_columns([
        pl.when(pl.col("QuantidadeRemessas") > 0)
        .then((pl.col("QtdEntregue") / pl.col("QuantidadeRemessas") * 100).round(2))
        .otherwise(0.0)
        .alias("PctEntregue")
    ])
    .sort("QuantidadeRemessas", descending=True)
)

# ==========================================================
# BASE PRINCIPAL POR ENTREGADOR (opcional, mas útil)
# A base "principal" será a de maior QuantidadeRemessas.
# Se houver empate, podem aparecer múltiplas bases.
# ==========================================================
base_principal = (
    resumo_base_motorista
    .with_columns(
        pl.col("QuantidadeRemessas")
        .rank("dense", descending=True)
        .over(COL_MOTORISTA)
        .alias("_rank")
    )
    .filter(pl.col("_rank") == 1)
    .drop("_rank")
    .rename({
        COL_BASE: "BasePrincipal",
        "QuantidadeRemessas": "QtdRemessasNaBasePrincipal",
        "QtdEntregue": "QtdEntregueNaBasePrincipal",
        "QtdNaoEntregue": "QtdNaoEntregueNaBasePrincipal",
        "PctEntregue": "PctEntregueNaBasePrincipal",
    })
    .sort("QtdRemessasNaBasePrincipal", descending=True)
)

# ==========================================================
# SALVAR
# ==========================================================
os.makedirs(os.path.dirname(ARQUIVO_SAIDA), exist_ok=True)

with pd.ExcelWriter(ARQUIVO_SAIDA, engine="xlsxwriter") as writer:
    resumo_base_motorista.to_pandas().to_excel(
        writer, sheet_name="Resumo Base x Entregador", index=False
    )
    base_principal.to_pandas().to_excel(
        writer, sheet_name="Base Principal por Entregador", index=False
    )

print("\n✅ Arquivo gerado com sucesso!")
print("📁 Local:", ARQUIVO_SAIDA)
