# -*- coding: utf-8 -*-
"""
📦 Comparativo Shipping Time Semanal — versão robusta final
---------------------------------------------------------------
- Corrige tipos numéricos automaticamente
- Usa PDD de Entrega como base
- Calcula Etapas 6, 7, 8 e Tempo Total
- Gera comparativo limpo e compatível
"""

import polars as pl
import os
import glob
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")

# =================== CONFIG ===================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\1. Shipping Time"
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

def limpar_coluna_num(df, col):
    """Limpa strings e converte para float."""
    return (
        df[col]
        .str.replace_all(r"[^\d,.\-]", "")  # remove letras e símbolos
        .str.replace(",", ".")               # vírgula -> ponto
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

    # limpa e converte valores
    df = df.with_columns([
        limpar_coluna_num(df, col6).alias(col6),
        limpar_coluna_num(df, col7).alias(col7),
        limpar_coluna_num(df, col8).alias(col8)
    ])

    # soma total
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
    return agrupado

def gerar_comparativo(semana_ant, semana_atual):
    comp = semana_ant.join(semana_atual, on="Base Entrega", how="outer", suffix="_Atual")

    # converte possíveis strings em float antes da subtração
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

    print("\n⏳ Calculando médias por base (PDD de Entrega)...")
    semana_atual = calcular_tempo_medio(df_atual)
    semana_anterior = calcular_tempo_medio(df_ant)

    print("📈 Gerando comparativo...")
    comparativo = gerar_comparativo(semana_anterior, semana_atual)
    resumo_final(semana_anterior, semana_atual)

    output_excel = os.path.join(OUTPUT_DIR, "Comparativo_ShippingTime.xlsx")
    comparativo.write_excel(output_excel)
    print(f"✅ Comparativo salvo em:\n{output_excel}\n")


if __name__ == "__main__":
    main()
