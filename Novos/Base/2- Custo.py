# -*- coding: utf-8 -*-

import pandas as pd
import os
from datetime import datetime

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================

BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Custo"
OUTPUT_PATH = os.path.join(BASE_DIR, "Minha_responsabilidade_atualizada.xlsx")
OUTPUT_SHARED = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Relatorios\Custos\Relatorio_Custos.xlsx"
COORDENADOR_PATH = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"

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

def to_float_safe(series):
    """Converte para float, ignorando erros."""
    return pd.to_numeric(series.astype(str).str.replace(",", ".").str.extract(r"(\d+\.?\d*)")[0], errors="coerce").fillna(0)

def gerar_nome_seguro(path):
    """Cria novo nome se o arquivo estiver bloqueado."""
    base, ext = os.path.splitext(path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base}_{timestamp}{ext}"

def encontrar_arquivo_entrada(pasta):
    """Encontra o primeiro arquivo Excel válido da pasta, ignorando os gerados pelo script."""
    arquivos = [
        f for f in os.listdir(pasta)
        if f.lower().endswith((".xls", ".xlsx"))
        and not f.startswith("~$")
        and not f.lower().startswith(("minha_responsabilidade", "relatorio_"))
    ]
    if not arquivos:
        raise FileNotFoundError("⚠️ Nenhum arquivo Excel válido encontrado na pasta de entrada.")
    # opcional: ordenar por data de modificação e pegar o mais recente
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
    print(f"📄 Planilha carregada com sucesso ({len(df):,} linhas)".replace(",", "."))

    total_inicial = len(df)

    # ------------------------------------------------------
    # 🔍 Filtra apenas Regionais GP
    # ------------------------------------------------------
    if "Regional responsável" in df.columns:
        df = df[df["Regional responsável"].astype(str).str.strip() == "GP"]
        print(f"✅ Linhas após filtro 'Regional responsável = GP': {len(df):,}".replace(",", "."))
    else:
        print("⚠️ Coluna 'Regional responsável' não encontrada. Nenhum filtro aplicado.")

    # ------------------------------------------------------
    # 💰 Calcula custo estimado
    # ------------------------------------------------------
    if "Tipo de anomalia primária" in df.columns:
        df["Custo Estimado"] = 0
        df.loc[df["Tipo de anomalia primária"].str.contains("Dano", na=False, case=False), "Custo Estimado"] = 50
        df.loc[df["Tipo de anomalia primária"].str.contains("Perdido", na=False, case=False), "Custo Estimado"] = 150
        df.loc[df["Tipo de anomalia primária"].str.contains("Atraso", na=False, case=False), "Custo Estimado"] = 10
        print("💵 Custo estimado adicionado com base nas anomalias.")
    else:
        df["Custo Estimado"] = 0
        print("⚠️ Coluna 'Tipo de anomalia primária' não encontrada. Custo estimado zerado.")

    # ------------------------------------------------------
    # 👥 Junta com coordenadores (se existir)
    # ------------------------------------------------------
    if os.path.exists(COORDENADOR_PATH):
        df_coord = pd.read_excel(COORDENADOR_PATH)
        if {"Nome da base", "Coordenadores"}.issubset(df_coord.columns):
            df = pd.merge(
                df,
                df_coord[["Nome da base", "Coordenadores"]],
                left_on="Base responsável",
                right_on="Nome da base",
                how="left"
            ).drop(columns=["Nome da base"], errors="ignore")
            print("👥 Coordenadores vinculados com sucesso.")
        else:
            print("⚠️ Planilha de coordenadores não contém as colunas esperadas.")
    else:
        print("⚠️ Planilha de coordenadores não encontrada.")

    # ------------------------------------------------------
    # 🧹 Remove remessas terminando em "-000" até "-999"
    # ------------------------------------------------------
    linhas_antes = len(df)
    valor_removido = 0.0

    if "Remessa" in df.columns:
        df["Remessa"] = df["Remessa"].astype(str).str.strip()
        df["Remessa"] = df["Remessa"].str.replace("–", "-", regex=False)
        padrao_remessa = r".*-\d{3}\s*$"

        if "Valor a pagar (yuan)" in df.columns:
            df["Valor a pagar (yuan)_num"] = to_float_safe(df["Valor a pagar (yuan)"])
            valor_removido = df.loc[df["Remessa"].str.match(padrao_remessa, na=False), "Valor a pagar (yuan)_num"].sum()

        df = df[~df["Remessa"].str.match(padrao_remessa, na=False)]

    linhas_removidas = linhas_antes - len(df)
    print(f"🧹 {linhas_removidas:,} linha(s) removida(s) com remessas terminando em '-000~999'".replace(",", "."))
    print(f"💸 Valor total removido: ¥ {format_currency(valor_removido)}")

    # ------------------------------------------------------
    # 🕒 Data de processamento
    # ------------------------------------------------------
    data_atual = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    df["Data de processamento de retorno"] = df.get("Data de processamento de retorno", pd.Series([None]*len(df)))
    df["Data de processamento de retorno"] = df["Data de processamento de retorno"].fillna(data_atual)
    print(f"📌 Data de processamento registrada: {data_atual}")

    # ------------------------------------------------------
    # 📑 Reordena colunas
    # ------------------------------------------------------
    if "Base responsável" in df.columns and "Coordenadores" in df.columns:
        cols = df.columns.tolist()
        nova_ordem = ["Base responsável", "Coordenadores"] + [c for c in cols if c not in ["Base responsável", "Coordenadores"]]
        df = df[nova_ordem]
        print("✅ Colunas reordenadas (Base responsável e Coordenadores primeiro).")

    # ------------------------------------------------------
    # 📊 Resumos
    # ------------------------------------------------------
    custo_total = df["Custo Estimado"].sum() if "Custo Estimado" in df.columns else 0
    resumo = pd.DataFrame({
        "Indicador": [
            "Total de linhas originais",
            "Total após limpeza",
            "Linhas removidas (-000~999)",
            "Valor total removido (¥)",
            "Custo total estimado (R$)",
            "Data de processamento"
        ],
        "Valor": [
            f"{total_inicial:,}".replace(",", "."),
            f"{len(df):,}".replace(",", "."),
            f"{linhas_removidas:,}".replace(",", "."),
            f"¥ {format_currency(valor_removido)}",
            format_currency(custo_total),
            data_atual
        ]
    })

    resumo_coord = pd.DataFrame()
    if {"Coordenadores", "Valor a pagar (yuan)", "Custo Estimado"}.issubset(df.columns):
        df["Valor a pagar (yuan)_num"] = to_float_safe(df["Valor a pagar (yuan)"])
        resumo_coord = (
            df.groupby(["Coordenadores", "Base responsável"], dropna=False)
            .agg({
                "Remessa": "count",
                "Valor a pagar (yuan)_num": "sum",
                "Custo Estimado": "sum"
            })
            .reset_index()
        )
        resumo_coord.rename(columns={
            "Remessa": "Total_Pedidos",
            "Valor a pagar (yuan)_num": "Valor_Total_Yuan",
            "Custo Estimado": "Custo_Total_R$"
        }, inplace=True)
        resumo_coord.sort_values(by="Valor_Total_Yuan", ascending=False, inplace=True)
        resumo_coord["Valor_Total_Yuan"] = resumo_coord["Valor_Total_Yuan"].apply(format_currency)
        resumo_coord["Custo_Total_R$"] = resumo_coord["Custo_Total_R$"].apply(format_currency)
        print("📈 Resumo por Coordenador gerado e ordenado pelo maior valor total (¥).")

    # ------------------------------------------------------
    # 💾 Salvar com proteção contra arquivo aberto
    # ------------------------------------------------------
    os.makedirs(os.path.dirname(OUTPUT_SHARED), exist_ok=True)
    final_output = OUTPUT_PATH

    try:
        with pd.ExcelWriter(final_output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Base_Processada")
            resumo.to_excel(writer, index=False, sheet_name="Resumo_Geral")
            if not resumo_coord.empty:
                resumo_coord.to_excel(writer, index=False, sheet_name="Resumo_por_Coordenador")
    except PermissionError:
        final_output = gerar_nome_seguro(OUTPUT_PATH)
        print(f"⚠️ Arquivo aberto. Salvando como nova versão: {os.path.basename(final_output)}")
        with pd.ExcelWriter(final_output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Base_Processada")
            resumo.to_excel(writer, index=False, sheet_name="Resumo_Geral")
            if not resumo_coord.empty:
                resumo_coord.to_excel(writer, index=False, sheet_name="Resumo_por_Coordenador")

    df.to_excel(OUTPUT_SHARED, index=False)

    print(f"\n✅ Arquivos salvos com sucesso!")
    print(f"   📁 Local: {final_output}")
    print(f"   ☁️ Compartilhado: {OUTPUT_SHARED}")
    print(f"\n📊 Resumo: {len(df):,} linhas finais | {linhas_removidas:,} removidas | Valor removido ¥ {format_currency(valor_removido)} | Custo total R$ {format_currency(custo_total)}".replace(",", "."))

except Exception as e:
    print(f"\n❌ Erro ao processar o arquivo:\n{e}")