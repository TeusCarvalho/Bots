# -*- coding: utf-8 -*-
"""
Comparativo Shipping Time - Setembro x Outubro (por Regional Destino)
Autor: bb (ChatGPT ❤️ Matheus)
Objetivo:
    - Ler todos os arquivos .xlsx de cada pasta (Setembro e Outubro)
    - Ignorar arquivos temporários (~$)
    - Cruzar com a base de Coordenadores usando "Regional Destino"
    - Calcular métricas por base (Etapas 6, 7 e 8)
    - Gerar comparativo e salvar em duas abas (Completo + Somente Etapas)
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# =============================================================================
# CONFIGURAÇÕES GERAIS
# =============================================================================

BASE_DIR_SETEMBRO = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho"
    r"\Testes\Local de Teste\ShippintTime\Setembro"
)

BASE_DIR_OUTUBRO = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho"
    r"\Testes\Local de Teste\ShippintTime\Outubro"
)

COORDENADOR_DIR = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador"
)

OUTPUT_FILE = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho"
    r"\Testes\Local de Teste\ShippintTime\Comparativo_ShippingTime_Setembro_Outubro.xlsx"
)

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def load_all_excels(directory):
    """Lê todos os arquivos .xlsx válidos da pasta e junta em um único DataFrame."""
    try:
        files = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.lower().endswith(".xlsx") and not f.startswith("~$")
        ]

        if not files:
            print(f"⚠️ Nenhum arquivo .xlsx válido encontrado em '{directory}'.")
            return pd.DataFrame()

        df_list = []
        print(f"📂 Encontrados {len(files)} arquivo(s) em '{directory}'\n")

        for f in files:
            try:
                print(f"📄 Lendo: {os.path.basename(f)} ...", end=" ")
                df = pd.read_excel(f, engine="openpyxl")
                print(f"{len(df)} linhas ✔️")
                df_list.append(df)
            except Exception as e:
                print(f"❌ Erro ao ler '{os.path.basename(f)}': {e}")

        if not df_list:
            print(f"⚠️ Nenhum arquivo pôde ser lido corretamente em '{directory}'.")
            return pd.DataFrame()

        df_final = pd.concat(df_list, ignore_index=True)
        print(f"\n✅ Total combinado: {len(df_final)} linhas (de {len(df_list)} arquivo(s) válidos).")
        return df_final

    except Exception as e:
        print(f"❌ Erro ao ler arquivos da pasta '{directory}': {e}")
        return pd.DataFrame()


def load_excel(filepath, name="Arquivo"):
    """Carrega um arquivo Excel em um DataFrame."""
    if not filepath:
        print(f"❌ {name} não encontrado.")
        return None
    try:
        df = pd.read_excel(filepath, engine="openpyxl")
        print(f"✅ {name} '{os.path.basename(filepath)}' carregado com sucesso ({len(df)} linhas).")
        return df
    except Exception as e:
        print(f"❌ Erro ao carregar {name}: {e}")
        return None


def map_data(df):
    """Padroniza e seleciona colunas necessárias."""
    mapped = pd.DataFrame({
        "base_entrega": df.get("Regional Destino", pd.Series(dtype="str")),
        "coordenador": df.get("Coordenadores", pd.Series(dtype="str")),
        "Etapa 6": df.get("Tempo trânsito SC Destino->Base Entrega"),
        "Etapa 7": df.get("Tempo médio processamento Base Entrega"),
        "Etapa 8": df.get("Tempo médio Saída para Entrega->Entrega"),
        "pedido": df.get("Número de pedido JMS", pd.Series(dtype="str")),
    })
    return mapped.fillna("N/D")


def calcular_por_base(df):
    """Retorna métricas agrupadas por base."""
    if df.empty:
        return pd.DataFrame()

    df["Etapa 6"] = pd.to_numeric(df["Etapa 6"], errors="coerce")
    df["Etapa 7"] = pd.to_numeric(df["Etapa 7"], errors="coerce")
    df["Etapa 8"] = pd.to_numeric(df["Etapa 8"], errors="coerce")

    resumo = df.groupby("base_entrega").agg(
        Qtd_Total_de_Pedidos=("pedido", "nunique"),
        Etapa_6=("Etapa 6", "mean"),
        Etapa_7=("Etapa 7", "mean"),
        Etapa_8=("Etapa 8", "mean")
    ).reset_index()

    return resumo


# =============================================================================
# FLUXO PRINCIPAL
# =============================================================================

def processar_mes(pasta_mes, nome_mes, df_coord):
    """Processa um mês específico (lendo todos os arquivos da pasta) e retorna métricas por base."""
    print(f"\n📂 Processando dados de {nome_mes}...")
    df = load_all_excels(pasta_mes)

    if df.empty:
        print(f"⚠️ Nenhum dado encontrado para {nome_mes}.")
        return pd.DataFrame()

    # Filtrar GP e cruzar coordenadores
    if "Regional de Entrega" in df.columns:
        df = df[df["Regional de Entrega"] == "GP"]

    df_merged = pd.merge(
        df,
        df_coord,
        left_on="Regional Destino",
        right_on="Nome da base",
        how="left"
    )

    df_processed = map_data(df_merged)
    resumo = calcular_por_base(df_processed)
    resumo["Mês"] = nome_mes

    # Mostrar quantidade de regionais analisadas
    qtd_bases = resumo["base_entrega"].nunique()
    print(f"📊 {qtd_bases} regionais analisadas em {nome_mes}.")
    return resumo


def main():
    print("=" * 80)
    print("🚚 Comparativo Shipping Time - Setembro x Outubro (por Regional Destino)")
    print("=" * 80)

    # Carregar base de coordenadores
    coord_files = [
        os.path.join(COORDENADOR_DIR, f)
        for f in os.listdir(COORDENADOR_DIR)
        if f.lower().endswith(".xlsx")
    ]
    if not coord_files:
        print("❌ Nenhum arquivo de coordenadores encontrado.")
        return
    coord_file = max(coord_files, key=os.path.getmtime)
    df_coord = load_excel(coord_file, "Base de Coordenadores")

    # Processar cada mês
    df_set = processar_mes(BASE_DIR_SETEMBRO, "Setembro", df_coord)
    df_out = processar_mes(BASE_DIR_OUTUBRO, "Outubro", df_coord)

    if df_set.empty or df_out.empty:
        print("❌ Um dos meses está vazio. Verifique as planilhas.")
        return

    # Criar comparativo
    print("\n📊 Gerando comparativo por Regional Destino (Etapas 6, 7 e 8)...")
    df_comp = pd.merge(df_set, df_out, on="base_entrega", how="outer", suffixes=("_Set", "_Out"))

    # Calcular variações
    for col in ["Qtd_Total_de_Pedidos", "Etapa_6", "Etapa_7", "Etapa_8"]:
        df_comp[f"Δ_{col}"] = df_comp[f"{col}_Out"] - df_comp[f"{col}_Set"]

    # Arredondar e preencher
    df_comp = df_comp.fillna(0)
    for col in [
        "Etapa_6_Set", "Etapa_6_Out",
        "Etapa_7_Set", "Etapa_7_Out",
        "Etapa_8_Set", "Etapa_8_Out"
    ]:
        df_comp[col] = df_comp[col].round(2)

    # Criar versão simplificada (somente etapas)
    df_etapas = df_comp[[
        "base_entrega",
        "Etapa_6_Set", "Etapa_6_Out", "Δ_Etapa_6",
        "Etapa_7_Set", "Etapa_7_Out", "Δ_Etapa_7",
        "Etapa_8_Set", "Etapa_8_Out", "Δ_Etapa_8"
    ]].copy()

    # Salvar resultado em duas abas
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df_comp.to_excel(writer, index=False, sheet_name="Comparativo Completo")
        df_etapas.to_excel(writer, index=False, sheet_name="Somente Etapas")

    print(f"\n✅ Comparativo salvo com sucesso em:\n{OUTPUT_FILE}")
    print("📘 Aba 1: Comparativo Completo (com Qtd Total de Pedidos + etapas)")
    print("📗 Aba 2: Somente Etapas (Etapa 6, 7 e 8, sem contagem)")
    print("=" * 80)


# =============================================================================
# EXECUÇÃO
# =============================================================================
if __name__ == "__main__":
    main()