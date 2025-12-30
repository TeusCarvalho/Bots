# -*- coding: utf-8 -*-
"""
===========================================================
🚚 Comparativo Shipping Time - Setembro x Outubro (Polars Lazy)
Autor: bb-assistente 😎
-----------------------------------------------------------
✅ Usa Polars Lazy Mode (10x mais rápido)
✅ Lê todos os arquivos de Setembro e Outubro
✅ Cruza com Base_Atualizada.xlsx (coordenadores)
✅ Calcula etapas 6, 7 e 8 por base
✅ Gera comparativo consolidado (Completo + Etapas)
===========================================================
"""

import os
import polars as pl
import pandas as pd
from datetime import datetime

# ============================================================
# ⚙️ CONFIGURAÇÕES
# ============================================================

BASE_DIR_SETEMBRO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\ShippintTime\Setembro"
BASE_DIR_OUTUBRO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\ShippintTime\Outubro"
COORDENADOR_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador"
OUTPUT_FILE = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\ShippintTime\Comparativo_ShippingTime_Setembro_Outubro.xlsx"

# ============================================================
# 🧠 FUNÇÕES AUXILIARES
# ============================================================

def ler_planilhas_polars(pasta: str) -> pl.DataFrame:
    """Lê todos os arquivos Excel válidos da pasta usando Polars Lazy."""
    arquivos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith(".xlsx") and not f.startswith("~$")
    ]

    if not arquivos:
        print(f"⚠️ Nenhum arquivo Excel encontrado em {pasta}")
        return pl.DataFrame()

    dfs_lazy = []
    for arq in arquivos:
        try:
            print(f"📄 Lendo: {os.path.basename(arq)}")
            df_lazy = pl.read_excel(arq).lazy()
            dfs_lazy.append(df_lazy)
        except Exception as e:
            print(f"❌ Erro ao ler {os.path.basename(arq)}: {e}")

    if not dfs_lazy:
        print(f"⚠️ Nenhum dado válido encontrado em {pasta}")
        return pl.DataFrame()

    df_total = pl.concat(dfs_lazy).collect()
    print(f"✅ {len(df_total)} linhas combinadas ({len(arquivos)} arquivos).")
    return df_total


def calcular_por_base(df: pl.DataFrame, nome_mes: str) -> pd.DataFrame:
    """Calcula métricas por base (Etapas 6,7,8) e retorna DataFrame Pandas."""
    if df.is_empty():
        return pd.DataFrame()

    df = df.with_columns([
        pl.col("Tempo trânsito SC Destino->Base Entrega").cast(pl.Float64).alias("Etapa 6"),
        pl.col("Tempo médio processamento Base Entrega").cast(pl.Float64).alias("Etapa 7"),
        pl.col("Tempo médio Saída para Entrega->Entrega").cast(pl.Float64).alias("Etapa 8"),
        pl.col("Número de pedido JMS").cast(pl.Utf8).alias("pedido"),
        pl.col("Regional Destino").cast(pl.Utf8).alias("base_entrega"),
    ])

    resumo = (
        df.group_by("base_entrega")
        .agg([
            pl.col("pedido").n_unique().alias("Qtd_Total_de_Pedidos"),
            pl.col("Etapa 6").mean().alias("Etapa_6"),
            pl.col("Etapa 7").mean().alias("Etapa_7"),
            pl.col("Etapa 8").mean().alias("Etapa_8"),
        ])
        .with_columns(pl.lit(nome_mes).alias("Mês"))
    )

    return resumo.to_pandas()


def processar_mes(pasta: str, nome_mes: str, df_coord: pl.DataFrame) -> pd.DataFrame:
    """Processa um mês (Setembro/Outubro) e retorna resumo por base."""
    print(f"\n📦 Processando {nome_mes}...")
    df_mes = ler_planilhas_polars(pasta)
    if df_mes.is_empty():
        return pd.DataFrame()

    # 🔎 Filtrar Regional GP, se existir
    if "Regional de Entrega" in df_mes.columns:
        df_mes = df_mes.filter(pl.col("Regional de Entrega") == "GP")

    # 🔗 Merge com coordenadores
    if "Regional Destino" in df_mes.columns:
        df_mes = df_mes.join(
            df_coord,
            left_on="Regional Destino",
            right_on="Nome da base",
            how="left"
        )

    resumo = calcular_por_base(df_mes, nome_mes)
    print(f"📊 {len(resumo)} bases analisadas ({nome_mes})")
    return resumo


# ============================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ============================================================

def main():
    print("=" * 80)
    print("🚚 Comparativo Shipping Time - Setembro x Outubro (Polars Lazy)")
    print("=" * 80)

    # 🧭 Lê a base de coordenadores (pandas -> polars)
    coord_files = [
        os.path.join(COORDENADOR_DIR, f)
        for f in os.listdir(COORDENADOR_DIR)
        if f.lower().endswith(".xlsx")
    ]
    if not coord_files:
        print("❌ Nenhum arquivo de coordenadores encontrado.")
        return

    coord_file = max(coord_files, key=os.path.getmtime)
    df_coord = pl.from_pandas(pd.read_excel(coord_file))
    print(f"✅ Base de Coordenadores '{os.path.basename(coord_file)}' carregada.\n")

    # 📆 Processar os dois meses
    df_set = processar_mes(BASE_DIR_SETEMBRO, "Setembro", df_coord)
    df_out = processar_mes(BASE_DIR_OUTUBRO, "Outubro", df_coord)

    if df_set.empty or df_out.empty:
        print("⚠️ Um dos meses não possui dados válidos.")
        return

    # 📈 Criar comparativo
    print("\n📊 Gerando comparativo...")
    df_comp = pd.merge(df_set, df_out, on="base_entrega", how="outer", suffixes=("_Set", "_Out"))

    # Calcular diferenças
    for col in ["Qtd_Total_de_Pedidos", "Etapa_6", "Etapa_7", "Etapa_8"]:
        df_comp[f"Δ_{col}"] = df_comp[f"{col}_Out"] - df_comp[f"{col}_Set"]

    # Arredondar
    for col in [
        "Etapa_6_Set", "Etapa_6_Out",
        "Etapa_7_Set", "Etapa_7_Out",
        "Etapa_8_Set", "Etapa_8_Out"
    ]:
        if col in df_comp.columns:
            df_comp[col] = df_comp[col].round(2)

    # Criar resumo simplificado
    df_etapas = df_comp[[
        "base_entrega",
        "Etapa_6_Set", "Etapa_6_Out", "Δ_Etapa_6",
        "Etapa_7_Set", "Etapa_7_Out", "Δ_Etapa_7",
        "Etapa_8_Set", "Etapa_8_Out", "Δ_Etapa_8"
    ]].copy()

    # 📁 Exportar Excel
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df_comp.to_excel(writer, index=False, sheet_name="Comparativo Completo")
        df_etapas.to_excel(writer, index=False, sheet_name="Somente Etapas")

    print(f"\n✅ Comparativo salvo com sucesso em:\n{OUTPUT_FILE}")
    print("📘 Aba 1: Comparativo Completo")
    print("📗 Aba 2: Somente Etapas")
    print("=" * 80)


# ============================================================
# ▶️ EXECUTAR
# ============================================================
if __name__ == "__main__":
    main()