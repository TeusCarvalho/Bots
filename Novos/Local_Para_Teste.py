# -*- coding: utf-8 -*-
"""
📦 Comparativo Shipping Time Semanal — versão final bb 😎
---------------------------------------------------------
Funções:
 - Lê os arquivos Excel (.xls e .xlsx) das duas últimas pastas semanais
 - Calcula médias por base (Etapas 6, 7, 8 e Total)
 - Exibe barra de progresso elegante (tqdm)
 - Mostra resumo final de variações (↑ / ↓)
 - Gera Excel final com o comparativo
"""

import polars as pl
import os
import glob
from tqdm import tqdm
import warnings

# 🔇 Desativar avisos de dtype e outros
warnings.filterwarnings("ignore", category=UserWarning)

# =====================================================================
# CONFIGURAÇÕES GERAIS
# =====================================================================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\1. Shipping Time"
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =====================================================================
# FUNÇÕES AUXILIARES
# =====================================================================

def encontrar_duas_ultimas_pastas(base_path: str):
    """Retorna as duas subpastas válidas (sem Output) mais recentes."""
    subpastas = [
        os.path.join(base_path, d)
        for d in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, d)) and "output" not in d.lower()
    ]
    subpastas.sort(key=os.path.getmtime, reverse=True)
    return subpastas[:2] if len(subpastas) >= 2 else []

def ler_todos_excel(pasta):
    """Lê e concatena todos os arquivos Excel (.xls e .xlsx) de uma pasta com barra de progresso."""
    arquivos = glob.glob(os.path.join(pasta, "*.xls*"))
    if not arquivos:
        print(f"⚠️ Nenhum arquivo Excel encontrado em: {pasta}")
        return None

    dfs = []
    print(f"\n📂 Lendo planilhas da pasta: {os.path.basename(pasta)}")
    for arq in tqdm(arquivos, desc="📊 Processando arquivos", unit="arquivo"):
        try:
            df = pl.read_excel(arq)
            dfs.append(df)
        except Exception:
            pass  # ignora erros e segue

    if not dfs:
        return None
    return pl.concat(dfs, how="vertical")

def calcular_tempo_medio(df):
    """Calcula médias por base para cada etapa e o total."""
    col6 = "Tempo trânsito SC Destino->Base Entrega"
    col7 = "Tempo médio processamento Base Entrega"
    col8 = "Tempo médio Saída para Entrega->Entrega"

    for col in [col6, col7, col8]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    df = df.with_columns([
        (pl.col(col6) + pl.col(col7) + pl.col(col8)).alias("Tempo Total (h)")
    ])

    agrupado = df.groupby("Base Entrega").agg([
        pl.mean(col6).alias("Etapa 6 (h)"),
        pl.mean(col7).alias("Etapa 7 (h)"),
        pl.mean(col8).alias("Etapa 8 (h)"),
        pl.mean("Tempo Total (h)").alias("Tempo Total (h)")
    ])
    return agrupado

def gerar_comparativo(semana_ant, semana_atual):
    """Compara semanas e calcula variação por etapa."""
    comp = semana_ant.join(semana_atual, on="Base Entrega", how="outer", suffix="_Atual")

    for etapa in ["Etapa 6", "Etapa 7", "Etapa 8", "Tempo Total"]:
        comp = comp.with_columns([
            (pl.col(f"{etapa} (h)_Atual") - pl.col(f"{etapa} (h)")).alias(f"{etapa} Δ (h)")
        ])
    return comp

def resumo_final(semana_ant, semana_atual):
    """Exibe resumo geral das variações entre semanas."""
    etapas = {
        "Shipping Time": "Tempo Total (h)",
        "Etapa 6": "Tempo trânsito SC Destino->Base Entrega",
        "Etapa 7": "Tempo médio processamento Base Entrega",
        "Etapa 8": "Tempo médio Saída para Entrega->Entrega"
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

# =====================================================================
# PROCESSAMENTO PRINCIPAL
# =====================================================================

def main():
    print("\n🚀 Iniciando análise comparativa semanal...")

    pastas = encontrar_duas_ultimas_pastas(BASE_DIR)
    if len(pastas) < 2:
        print("❌ Menos de duas pastas encontradas. Abortando.")
        return

    semana_atual_path, semana_anterior_path = pastas[0], pastas[1]
    print(f"📁 Semana Atual: {os.path.basename(semana_atual_path)}")
    print(f"📁 Semana Anterior: {os.path.basename(semana_anterior_path)}")

    # --- Ler dados ---
    df_atual = ler_todos_excel(semana_atual_path)
    df_ant = ler_todos_excel(semana_anterior_path)
    if df_atual is None or df_ant is None:
        print("❌ Não foi possível ler uma das semanas.")
        return

    # --- Padronizar coluna de base ---
    if "Base Entrega" not in df_atual.columns:
        if "Regional Destino" in df_atual.columns:
            df_atual = df_atual.rename({"Regional Destino": "Base Entrega"})
            df_ant = df_ant.rename({"Regional Destino": "Base Entrega"})
        else:
            print("❌ Nenhuma coluna de base encontrada.")
            return

    # --- Calcular médias ---
    print("\n⏳ Calculando médias por base...")
    semana_atual = calcular_tempo_medio(df_atual)
    semana_anterior = calcular_tempo_medio(df_ant)

    # --- Gerar comparativo ---
    print("📈 Gerando comparativo...")
    comparativo = gerar_comparativo(semana_anterior, semana_atual)

    # --- Resumo final ---
    resumo_final(semana_anterior, semana_atual)

    # --- Exportar Excel ---
    output_excel = os.path.join(OUTPUT_DIR, "Comparativo_ShippingTime.xlsx")
    comparativo.write_excel(output_excel)
    print(f"✅ Comparativo salvo em:\n{output_excel}\n")

if __name__ == "__main__":
    main()
