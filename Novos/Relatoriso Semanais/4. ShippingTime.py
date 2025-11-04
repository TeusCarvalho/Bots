# -*- coding: utf-8 -*-
"""
📦 Comparativo Shipping Time Semanal — FINAL + médias por dia (detecção automática)
---------------------------------------------------------------
- Corrige tipos numéricos automaticamente
- Usa PDD de Entrega como base
- Calcula Etapas 6, 7, 8 e Tempo Total
- Gera comparativo limpo e compatível
- Mantém apenas UFs especificadas em 'Estado de Entrega'
- Mostra TOP ofensores (Etapas 7 e 8)
- Adiciona linha TOTAL GERAL
- Cria aba "Média por Dia (Atual)" automaticamente se encontrar data
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

# =================== UF PERMITIDAS ===================
UFS_PERMITIDAS = ["PA", "MT", "GO", "AM", "MS", "RO", "TO", "DF", "RR", "AC", "AP"]

# =================== FUNÇÕES ===================

def encontrar_duas_ultimas_pastas(base_path):
    pastas = [
        os.path.join(base_path, d)
        for d in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, d)) and "output" not in d.lower()
    ]
    pastas.sort(key=os.path.getmtime, reverse=True)
    return pastas[:2] if len(pastas) >= 2 else []

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

    if not dfs:
        return None

    df_final = pl.concat(dfs, how="vertical")
    df_final = df_final.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in df_final.columns])
    return df_final

def filtrar_por_uf(df):
    if "Estado de Entrega" not in df.columns:
        print("⚠️ Coluna 'Estado de Entrega' não encontrada — mantendo todas as linhas.")
        return df
    df = df.filter(pl.col("Estado de Entrega").is_in(UFS_PERMITIDAS))
    print(f"✅ Linhas mantidas apenas das UFs: {', '.join(UFS_PERMITIDAS)}")
    return df

def limpar_coluna_num(df, col):
    """Limpa strings e converte para float."""
    return (
        df[col]
        .str.replace_all(r"[^\d,.\-]", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
        .fill_null(0)
        .fill_nan(0)
    )

def calcular_tempo_medio(df):
    base_col = "PDD de Entrega"
    col6 = "Tempo trânsito SC Destino->Base Entrega"
    col7 = "Tempo médio processamento Base Entrega"
    col8 = "Tempo médio Saída para Entrega->Entrega"

    for col in [base_col, col6, col7, col8]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    df = df.with_columns([
        limpar_coluna_num(df, col6).alias(col6),
        limpar_coluna_num(df, col7).alias(col7),
        limpar_coluna_num(df, col8).alias(col8)
    ])

    df = df.with_columns([
        (pl.col(col6) + pl.col(col7) + pl.col(col8)).alias("Tempo Total (h)")
    ])

    agrupado = (
        df.group_by(base_col)
        .agg([
            pl.mean(col6).alias("Etapa 6 (h)"),
            pl.mean(col7).alias("Etapa 7 (h)"),
            pl.mean(col8).alias("Etapa 8 (h)"),
            pl.mean("Tempo Total (h)").alias("Tempo Total (h)")
        ])
        .rename({base_col: "Base Entrega"})
    )

    # 🧮 Linha TOTAL GERAL
    total_geral = (
        agrupado.select([
            pl.lit("TOTAL GERAL").alias("Base Entrega"),
            pl.col("Etapa 6 (h)").mean(),
            pl.col("Etapa 7 (h)").mean(),
            pl.col("Etapa 8 (h)").mean(),
            pl.col("Tempo Total (h)").mean()
        ])
    )
    agrupado = pl.concat([agrupado, total_geral], how="vertical")

    return agrupado, df  # retorna também o original limpo

def detectar_coluna_data(df):
    """Tenta detectar automaticamente uma coluna de data."""
    possiveis = [c for c in df.columns if "data" in c.lower()]
    if possiveis:
        return possiveis[0]
    return None

def calcular_media_por_dia(df):
    col_data = detectar_coluna_data(df)
    if not col_data:
        print("⚠️ Nenhuma coluna de data encontrada — não foi possível gerar médias diárias.")
        return None

    df = df.with_columns([
        pl.col(col_data).str.slice(0, 10).alias("Data")
    ])

    media_dia = (
        df.group_by("Data")
        .agg([
            pl.mean("Tempo trânsito SC Destino->Base Entrega").alias("Etapa 6 (h)"),
            pl.mean("Tempo médio processamento Base Entrega").alias("Etapa 7 (h)"),
            pl.mean("Tempo médio Saída para Entrega->Entrega").alias("Etapa 8 (h)"),
            pl.mean("Tempo Total (h)").alias("Tempo Total (h)")
        ])
        .sort("Data")
    )
    return media_dia

def gerar_comparativo(semana_ant, semana_atual):
    comp = semana_ant.join(semana_atual, on="Base Entrega", how="outer", suffix="_Atual")

    for etapa in ["Etapa 6", "Etapa 7", "Etapa 8", "Tempo Total"]:
        comp = comp.with_columns([
            pl.col(f"{etapa} (h)").cast(pl.Float64, strict=False).alias(f"{etapa} (h)"),
            pl.col(f"{etapa} (h)_Atual").cast(pl.Float64, strict=False).alias(f"{etapa} (h)_Atual")
        ])
        comp = comp.with_columns([
            (pl.col(f"{etapa} (h)_Atual") - pl.col(f"{etapa} (h)")).alias(f"{etapa} Δ (h)")
        ])
    return comp

def resumo_final(semana_ant, semana_atual):
    etapas = {
        "Shipping Time": "Tempo Total (h)",
        "Etapa 6": "Etapa 6 (h)",
        "Etapa 7": "Etapa 7 (h)",
        "Etapa 8": "Etapa 8 (h)"
    }
    print("\n📊 **Resumo Semanal:**")
    for nome, col in etapas.items():
        if col in semana_ant.columns and col in semana_atual.columns:
            media_ant = semana_ant[col].mean()
            media_at = semana_atual[col].mean()
            diff = media_at - media_ant
            arrow = "↑" if diff > 0 else "↓"
            print(f"- {nome}: {media_at:.2f}h ({arrow}{abs(diff):.2f}h)")
    print("")

def mostrar_top_ofensores(comp, etapa="Etapa 7 (h)", top_n=10):
    base_col = "Base Entrega"
    atual_col = f"{etapa}_Atual"
    ant_col = etapa
    delta_col = f"{etapa.split(' (')[0]} Δ (h)"

    if not all(c in comp.columns for c in [base_col, ant_col, atual_col, delta_col]):
        print(f"⚠️ Colunas necessárias não encontradas para {etapa}.")
        return

    ofensores = (
        comp.select([base_col, ant_col, atual_col, delta_col])
        .drop_nulls(delta_col)
        .sort(delta_col, descending=True)
        .head(top_n)
    )

    print(f"\n🔥 Top {top_n} Ofensores — {etapa}:")
    print(f"{'Base Entrega':<25} {'Ant. (h)':>10} {'Atu. (h)':>10} {'Δ (h)':>10}")
    print("-" * 60)
    for row in ofensores.iter_rows():
        base, ant, atual, delta = row
        print(f"{base:<25} {ant:>10.2f} {atual:>10.2f} {delta:>10.2f}")

# =================== EXECUÇÃO ===================

def main():
    print("\n🚀 Iniciando análise comparativa semanal...")

    pastas = encontrar_duas_ultimas_pastas(BASE_DIR)
    if len(pastas) < 2:
        print("❌ Menos de duas pastas encontradas.")
        return

    semana_atual_path, semana_anterior_path = pastas[0], pastas[1]
    print(f"📁 Semana Atual: {os.path.basename(semana_atual_path)}")
    print(f"📁 Semana Anterior: {os.path.basename(semana_anterior_path)}")

    df_atual = ler_todos_excel(semana_atual_path)
    df_ant = ler_todos_excel(semana_anterior_path)
    if df_atual is None or df_ant is None:
        print("❌ Não foi possível ler uma das semanas.")
        return

    df_atual = filtrar_por_uf(df_atual)
    df_ant = filtrar_por_uf(df_ant)

    print("\n⏳ Calculando médias por base (PDD de Entrega)...")
    semana_atual, df_atual_limpo = calcular_tempo_medio(df_atual)
    semana_anterior, _ = calcular_tempo_medio(df_ant)

    print("📈 Gerando comparativo...")
    comparativo = gerar_comparativo(semana_anterior, semana_atual)
    resumo_final(semana_anterior, semana_atual)

    mostrar_top_ofensores(comparativo, "Etapa 7 (h)", top_n=10)
    mostrar_top_ofensores(comparativo, "Etapa 8 (h)", top_n=10)

    # 📅 Média por dia (semana atual)
    media_por_dia = calcular_media_por_dia(df_atual_limpo)

    # 💾 Exportação com pandas.ExcelWriter
    output_excel = os.path.join(OUTPUT_DIR, "Comparativo_ShippingTime.xlsx")
    with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
        comparativo.to_pandas().to_excel(writer, sheet_name="Comparativo Semanal", index=False)
        if media_por_dia is not None:
            media_por_dia.to_pandas().to_excel(writer, sheet_name="Média por Dia (Atual)", index=False)

    print(f"\n✅ Comparativo salvo em:\n{output_excel}\n")


if __name__ == "__main__":
    main()
