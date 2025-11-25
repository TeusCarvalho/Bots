# -*- coding: utf-8 -*-
"""
📦 Comparativo Shipping Time Semanal — FINAL v2.6.1
---------------------------------------------------------------
- Corrige tipos numéricos automaticamente
- Calcula Etapas 6, 7, 8 e Tempo Total
- Gera comparativo limpo e compatível
- Filtra UFs
- MOSTRA:
    ✔ Maior piora (tratado para None)
    ✔ Maior melhora (tratado para None)
    ✔ Comparativo Geral com Δ
    ✔ TOP 5 das Etapas com Δ
    ✔ Resumo diário
- Cria abas separadas por Data
- Divide automaticamente base consolidada
"""

import polars as pl
import pandas as pd
import os
import glob
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")

# =================== CONFIG ===================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\1. Shipping Time"
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

UFS_PERMITIDAS = ["PA", "MT", "GO", "AM", "MS", "RO", "TO", "DF", "RR", "AC", "AP"]
LIMITE_EXCEL = 1_048_000

# COLUNAS
COL_PDD_ENTREGA = "PDD de Entrega"
COL_ESTADO_ENTREGA = "Estado de Entrega"
COL_DATA = "Data"
COL_ETAPA_6 = "Tempo trânsito SC Destino->Base Entrega"
COL_ETAPA_7 = "Tempo médio processamento Base Entrega"
COL_ETAPA_8 = "Tempo médio Saída para Entrega->Entrega"
COL_TEMPO_TOTAL = "Tempo Total (h)"

# =================== FUNÇÕES ===================

def encontrar_duas_ultimas_pastas(path):
    pastas = [
        os.path.join(path, p)
        for p in os.listdir(path)
        if os.path.isdir(os.path.join(path, p)) and "output" not in p.lower()
    ]
    pastas.sort(key=os.path.getmtime, reverse=True)
    return pastas[:2]


def ler_todos_excel(pasta):
    arquivos = [
        arq for arq in glob.glob(os.path.join(pasta, "*.xls*"))
        if not os.path.basename(arq).startswith("~$")
    ]

    if not arquivos:
        print(f"⚠️ Nenhum arquivo Excel encontrado em: {pasta}")
        return None

    print(f"\n📂 Lendo planilhas da pasta: {os.path.basename(pasta)}")
    dfs = []

    for arq in tqdm(arquivos, desc="📊 Processando arquivos", unit="arquivo"):
        try:
            df = pl.read_excel(arq)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Erro ao ler {os.path.basename(arq)}: {e}")

    final = pl.concat(dfs, how="vertical")
    final = final.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in final.columns])

    return final


def filtrar_por_uf(df):
    if COL_ESTADO_ENTREGA not in df.columns:
        print(f"⚠️ Coluna '{COL_ESTADO_ENTREGA}' não encontrada.")
        return df

    df = df.filter(pl.col(COL_ESTADO_ENTREGA).is_in(UFS_PERMITIDAS))
    print("✅ UFs filtradas:", ", ".join(UFS_PERMITIDAS))
    return df


def limpar_coluna_num(df, col):
    return (
        df[col]
        .str.replace_all(r"[^\d,.\-]", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
        .fill_nan(0)
        .fill_null(0)
    )


def calcular_tempo_medio(df):

    for col in [COL_ETAPA_6, COL_ETAPA_7, COL_ETAPA_8]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    df = df.with_columns([
        limpar_coluna_num(df, COL_ETAPA_6).alias(COL_ETAPA_6),
        limpar_coluna_num(df, COL_ETAPA_7).alias(COL_ETAPA_7),
        limpar_coluna_num(df, COL_ETAPA_8).alias(COL_ETAPA_8),
    ])

    df = df.with_columns([
        (pl.col(COL_ETAPA_6) + pl.col(COL_ETAPA_7) + pl.col(COL_ETAPA_8)).alias(COL_TEMPO_TOTAL)
    ])

    agrupado = (
        df.group_by(COL_PDD_ENTREGA)
        .agg([
            pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
            pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
            pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
            pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
        ])
        .rename({COL_PDD_ENTREGA: "Base Entrega"})
    )

    total = agrupado.select([
        pl.lit("TOTAL GERAL").alias("Base Entrega"),
        pl.col("Etapa 6 (h)").mean(),
        pl.col("Etapa 7 (h)").mean(),
        pl.col("Etapa 8 (h)").mean(),
        pl.col("Tempo Total (h)").mean()
    ])

    return pl.concat([agrupado, total], how="vertical"), df


def gerar_comparativo(ant, atual):
    comp = ant.join(atual, on="Base Entrega", how="outer", suffix="_Atual")

    for etapa in ["Etapa 6", "Etapa 7", "Etapa 8", "Tempo Total"]:
        comp = comp.with_columns([
            (pl.col(f"{etapa} (h)_Atual") - pl.col(f"{etapa} (h)"))
            .cast(pl.Float64, strict=False)
            .alias(f"{etapa} Δ (h)")
        ])

    return comp


def calcular_media_por_dia(df):

    if COL_DATA not in df.columns:
        return None

    df = df.with_columns(pl.col(COL_DATA).str.slice(0, 10).alias(COL_DATA))

    return (
        df.group_by(COL_DATA)
        .agg([
            pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
            pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
            pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
            pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
        ])
        .sort(COL_DATA)
    )


def separar_por_data(df):

    if COL_DATA not in df.columns:
        return {}

    df = df.with_columns(pl.col(COL_DATA).str.slice(0, 10).alias(COL_DATA))
    datas = df.select(COL_DATA).unique().to_series().to_list()

    out = {}
    for d in datas:
        sub = df.filter(pl.col(COL_DATA) == d)
        out[d] = (
            sub.group_by(COL_PDD_ENTREGA)
            .agg([
                pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
                pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
                pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
                pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
            ])
            .rename({COL_PDD_ENTREGA: "Base Entrega"})
        )

    return out


def exportar_base_consolidada(writer, df):
    total = df.height
    abas = (total // LIMITE_EXCEL) + 1

    print(f"🧾 Base consolidada: {total:,} linhas → {abas} abas")

    for i in range(abas):
        ini = i * LIMITE_EXCEL
        fim = min((i + 1)*LIMITE_EXCEL, total)
        aba = f"Base Consolidada {i+1}"
        df.slice(ini, fim - ini).to_pandas().to_excel(writer, aba, index=False)


# =================================================================
# 🔥 RESUMO EXECUTIVO COM TRATAMENTO DE None (v2.6.1)
# =================================================================

def mostrar_resumo_executivo(comp_df, sem_at_df, media_dia_df=None):

    print("\n" + "="*70)
    print("📊 --- RESUMO EXECUTIVO ---".center(70))
    print("="*70)

    if not isinstance(comp_df, pl.DataFrame):
        comp_df = pl.from_pandas(comp_df)

    if not isinstance(sem_at_df, pl.DataFrame):
        sem_at_df = pl.from_pandas(sem_at_df)

    bases = comp_df.filter(pl.col("Base Entrega") != "TOTAL GERAL")

    # ------------------------------
    # MAIOR PIORA (COM TRATAMENTO)
    # ------------------------------
    piora = bases.sort("Tempo Total Δ (h)", descending=True).head(1)

    if not piora.is_empty():
        delta = piora['Tempo Total Δ (h)'][0]
        delta = 0 if delta is None else float(delta)
        print(f"🟥 MAIOR PIORA: {piora['Base Entrega'][0]} (+{delta:.2f}h)")

    # ------------------------------
    # MAIOR MELHORA (COM TRATAMENTO)
    # ------------------------------
    melhora = bases.sort("Tempo Total Δ (h)").head(1)

    if not melhora.is_empty():
        delta = melhora['Tempo Total Δ (h)'][0]
        delta = 0 if delta is None else float(delta)
        print(f"🟩 MAIOR MELHORA: {melhora['Base Entrega'][0]} ({delta:.2f}h)")

    # ------------------------------
    # COMPARATIVO GERAL
    # ------------------------------
    print("\n" + "-"*70)
    print("📦 COMPARATIVO GERAL — Semana Atual vs Anterior".center(70))
    print("-"*70)

    ant = comp_df.filter(pl.col("Base Entrega") == "TOTAL GERAL")
    at = sem_at_df.filter(pl.col("Base Entrega") == "TOTAL GERAL")

    if not ant.is_empty() and not at.is_empty():

        def seta(v): return "↑" if v > 0 else "↓"

        e6 = at["Etapa 6 (h)"][0]
        e7 = at["Etapa 7 (h)"][0]
        e8 = at["Etapa 8 (h)"][0]
        tot = at["Tempo Total (h)"][0]

        d6 = e6 - ant["Etapa 6 (h)"][0]
        d7 = e7 - ant["Etapa 7 (h)"][0]
        d8 = e8 - ant["Etapa 8 (h)"][0]
        dt = tot - ant["Tempo Total (h)"][0]

        print(f"Shipping Time: {tot:.2f}h ({seta(dt)}{abs(dt):.2f}h)")
        print(f"Etapa 6:       {e6:.2f}h ({seta(d6)}{abs(d6):.2f}h)")
        print(f"Etapa 7:       {e7:.2f}h ({seta(d7)}{abs(d7):.2f}h)")
        print(f"Etapa 8:       {e8:.2f}h ({seta(d8)}{abs(d8):.2f}h)")

    # ------------------------------
    # TOP 5 COM VARIAÇÃO Δ
    # ------------------------------
    print("\n" + "-"*70)
    print("🚨 TOP 5 OFENSORES (com Δ)".center(70))
    print("-"*70)

    def top5(col, titulo):
        print(f"\n{titulo}:")
        at = sem_at_df.filter(pl.col("Base Entrega") != "TOTAL GERAL")

        top = at.sort(col, descending=True).head(5)

        for row in top.iter_rows(named=True):
            base = row["Base Entrega"]
            atual = row[col]

            comp_row = comp_df.filter(pl.col("Base Entrega") == base)

            if comp_row.is_empty():
                print(f"  - {base}: {atual:.2f}h (Δ N/A)")
                continue

            nome_delta = f"{col.replace(' (h)', '')} Δ (h)"
            delta = comp_row[nome_delta][0]

            if delta is None or delta != delta:  # None ou NaN
                print(f"  - {base}: {atual:.2f}h (Δ N/A)")
            else:
                seta = "↑" if delta > 0 else "↓"
                print(f"  - {base}: {atual:.2f}h ({seta}{abs(delta):.2f}h)")

    top5("Etapa 6 (h)", "🔹 TOP 5 - ETAPA 6 (SC Destino -> Base Entrega)")
    top5("Etapa 7 (h)", "🔹 TOP 5 - ETAPA 7 (Processamento na Base)")
    top5("Etapa 8 (h)", "🔹 TOP 5 - ETAPA 8 (Saída para Entrega)")

    # ------------------------------
    # RESUMO DIÁRIO
    # ------------------------------
    if media_dia_df is not None:
        print("\n" + "-"*70)
        print("📆 MÉDIAS DIÁRIAS — Semana Atual".center(70))
        print("-"*70)

        for row in media_dia_df.iter_rows(named=True):
            print(f"\n📅 {row['Data']}")
            print(f"  - Etapa 6: {row['Etapa 6 (h)']:.2f}h")
            print(f"  - Etapa 7: {row['Etapa 7 (h)']:.2f}h")
            print(f"  - Etapa 8: {row['Etapa 8 (h)']:.2f}h")
            print(f"  - Tempo Total: {row['Tempo Total (h)']:.2f}h")

    print("="*70)


# =================== MAIN ===================

def main():
    print("\n🚀 Iniciando análise...")

    pastas = encontrar_duas_ultimas_pastas(BASE_DIR)
    if len(pastas) < 2:
        print("❌ Não há duas semanas para comparar.")
        return

    sem_atual_pasta, sem_ant_pasta = pastas[0], pastas[1]

    df_atual = ler_todos_excel(sem_atual_pasta)
    df_ant = ler_todos_excel(sem_ant_pasta)

    if df_atual is None or df_ant is None:
        print("❌ Falha ao ler semanas.")
        return

    df_atual = filtrar_por_uf(df_atual)
    df_ant = filtrar_por_uf(df_ant)

    sem_at, df_atual_limpo = calcular_tempo_medio(df_atual)
    sem_ant, _ = calcular_tempo_medio(df_ant)

    comp = gerar_comparativo(sem_ant, sem_at)

    media_dia = calcular_media_por_dia(df_atual_limpo)
    por_data = separar_por_data(df_atual_limpo)

    output = os.path.join(OUTPUT_DIR, "Comparativo_ShippingTime_PorData.xlsx")
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        comp.to_pandas().to_excel(writer, "Comparativo Semanal", index=False)

        if media_dia is not None:
            media_dia.to_pandas().to_excel(writer, "Média por Dia", index=False)

        for d, df_dia in por_data.items():
            aba = d.replace("/", "-")[:31]
            df_dia.to_pandas().to_excel(writer, aba, index=False)

        exportar_base_consolidada(writer, df_atual_limpo)

    print(f"\n📁 Arquivo salvo em:\n{output}\n")

    mostrar_resumo_executivo(comp, sem_at, media_dia)


if __name__ == "__main__":
    main()
