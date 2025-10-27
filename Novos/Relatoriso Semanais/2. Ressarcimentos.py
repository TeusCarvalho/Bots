# -*- coding: utf-8 -*-
"""
💰 Processamento de Custos - v2.3
Com separação de valores e quantidade por tipo de anomalia
"""

import pandas as pd
import os
from datetime import datetime

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================

BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\4. Ressarcimentos"
OUTPUT_PATH = os.path.join(BASE_DIR, "Minha_responsabilidade_atualizada.xlsx")

# ======================================================
# 🧠 FUNÇÕES AUXILIARES
# ======================================================

def format_currency(value):
    """Formata número em BRL (R$ 1.234,56)."""
    try:
        return f"{float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "0,00"

def carregar_excel(path):
    """Lê o arquivo Excel automaticamente (.xls ou .xlsx)."""
    try:
        return pd.read_excel(path, dtype=str, engine="openpyxl")
    except Exception:
        try:
            return pd.read_excel(path, dtype=str, engine="xlrd")
        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo Excel: {e}")

def gerar_nome_seguro(path):
    """Cria novo nome se o arquivo estiver bloqueado."""
    base, ext = os.path.splitext(path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base}_{timestamp}{ext}"

def encontrar_arquivo_entrada(pasta):
    """Encontra o primeiro arquivo Excel válido da pasta."""
    arquivos = [
        f for f in os.listdir(pasta)
        if f.lower().endswith((".xls", ".xlsx"))
        and not f.startswith("~$")
        and not f.lower().startswith(("minha_responsabilidade", "relatorio_"))
    ]
    if not arquivos:
        raise FileNotFoundError("⚠️ Nenhum arquivo Excel válido encontrado na pasta.")
    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(pasta, f)), reverse=True)
    return os.path.join(pasta, arquivos[0])

# ======================================================
# 🚀 PROCESSAMENTO PRINCIPAL
# ======================================================

try:
    print(f"📂 Procurando arquivo Excel em: {BASE_DIR}")
    FILE_PATH = encontrar_arquivo_entrada(BASE_DIR)
    print(f"✅ Arquivo selecionado: {os.path.basename(FILE_PATH)}\n")

    df = carregar_excel(FILE_PATH)
    total_inicial = len(df)
    print(f"📄 Planilha carregada com sucesso ({total_inicial:,} linhas)".replace(",", "."))

    # ------------------------------------------------------
    # 🔍 Filtro por Regional = GP
    # ------------------------------------------------------
    if "Regional responsável" in df.columns:
        df = df[df["Regional responsável"].astype(str).str.strip() == "GP"]
        print(f"🧭 Filtro aplicado: 'Regional responsável = GP' ({len(df):,} linhas)".replace(",", "."))
    else:
        print("⚠️ Coluna 'Regional responsável' não encontrada. Nenhum filtro aplicado.")

    # ------------------------------------------------------
    # 💰 Renomeia coluna de valor
    # ------------------------------------------------------
    if "Valor a pagar (yuan)" in df.columns:
        df.rename(columns={"Valor a pagar (yuan)": "Valor a pagar (R$)"}, inplace=True)

    # Converte coluna para número
    if "Valor a pagar (R$)" in df.columns:
        df["Valor a pagar (R$)"] = pd.to_numeric(df["Valor a pagar (R$)"], errors="coerce").fillna(0)

    # ------------------------------------------------------
    # 🧹 Remove remessas terminando em -000 até -999
    # ------------------------------------------------------
    linhas_antes = len(df)
    if "Remessa" in df.columns:
        df["Remessa"] = df["Remessa"].astype(str).str.strip().str.replace("–", "-", regex=False)
        removidas = df["Remessa"].str.match(r".*-\d{3}\s*$", na=False)
        df = df[~removidas]
        linhas_removidas = linhas_antes - len(df)
    else:
        linhas_removidas = 0

    # ------------------------------------------------------
    # 🧾 Totais gerais e por tipo de anomalia
    # ------------------------------------------------------
    valor_total = df["Valor a pagar (R$)"].sum() if "Valor a pagar (R$)" in df.columns else 0

    valores_por_tipo = {}
    quantidades_por_tipo = {}

    if "Tipo de anomalia primária" in df.columns:
        tipos_interesse = [
            "AVARIA 破损",
            "EXTRAVIO-遗失",
            "Reivindicações rápidas/投诉理赔"
        ]
        for tipo in tipos_interesse:
            filtro = df["Tipo de anomalia primária"].astype(str).str.contains(tipo, na=False, case=False)
            valores_por_tipo[tipo] = df.loc[filtro, "Valor a pagar (R$)"].sum()
            quantidades_por_tipo[tipo] = filtro.sum()

    # ------------------------------------------------------
    # 🕒 Data de processamento
    # ------------------------------------------------------
    data_atual = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    df["Data de processamento de retorno"] = data_atual

    # ------------------------------------------------------
    # 💾 Salvamento
    # ------------------------------------------------------
    try:
        with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Base_Processada")
    except PermissionError:
        new_output = gerar_nome_seguro(OUTPUT_PATH)
        print(f"⚠️ Arquivo aberto. Salvando como nova versão: {os.path.basename(new_output)}")
        with pd.ExcelWriter(new_output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Base_Processada")
        OUTPUT_PATH = new_output

    # ------------------------------------------------------
    # 📊 Resumo final no terminal
    # ------------------------------------------------------
    print("\n" + "="*60)
    print("📊 RESUMO DO PROCESSAMENTO")
    print("="*60)
    print(f"📄 Linhas originais:              {total_inicial:,}".replace(",", "."))
    print(f"✅ Linhas após limpeza:          {len(df):,}".replace(",", "."))
    print(f"🗑️  Linhas removidas (-000~999):  {linhas_removidas:,}".replace(",", "."))
    print(f"💴 Total geral (R$):             {format_currency(valor_total)}\n")

    print("💥 Valores e quantidades por tipo de anomalia:")
    for tipo in valores_por_tipo.keys():
        valor = valores_por_tipo[tipo]
        qtd = quantidades_por_tipo[tipo]
        print(f"   - {tipo}: R$ {format_currency(valor)}  |  {qtd:,} pedidos".replace(",", "."))

    print(f"\n🕒 Data de processamento:        {data_atual}")
    print(f"💾 Arquivo salvo em:             {OUTPUT_PATH}")
    print("="*60 + "\n")

except Exception as e:
    print(f"\n❌ Erro ao processar o arquivo:\n{e}")
