# -*- coding: utf-8 -*-
"""
📊 Consolidação e Soma por Nome da Base — Política de Bonificação T0
--------------------------------------------------------------------
• Lê todos os arquivos .xlsx da pasta especificada
• Mantém: Nome da base, Total Recebido, Entregue
• Calcula: SLA (%), Classificação, Pontuação e Elegibilidade
• Agrupa e soma por Nome da base
• Salva o resultado em:
  C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\Resultados
"""

import pandas as pd
import os

# ==========================================================
# 📂 Caminhos
# ==========================================================
PASTA_ORIGEM = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho"
    r"\Testes\Politicas de Bonificação\01 - Taxa de entrega T0"
)

PASTA_DESTINO = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho"
    r"\Testes\Politicas de Bonificação\Resultados"
)

# Cria a pasta de resultados, se não existir
os.makedirs(PASTA_DESTINO, exist_ok=True)

# ==========================================================
# 🧮 Funções de cálculo
# ==========================================================
def calcular_sla(entregue, recebido):
    """Calcula o SLA como percentual numérico"""
    try:
        if pd.isna(recebido) or recebido == 0:
            return 0
        return (entregue / recebido) * 100
    except Exception:
        return 0

def classificar_meta(sla):
    """Classifica conforme meta"""
    if sla < 95:
        return "Fora da Meta"
    elif sla < 97:
        return "Meta"
    else:
        return "Desafio"

def pontuacao_meta(sla):
    """Define pontuação com base no SLA"""
    if sla < 95:
        return 0.0
    elif sla < 97:
        return 1.0
    else:
        return 1.1

# ==========================================================
# 🚀 Leitura e Consolidação
# ==========================================================
arquivos = [f for f in os.listdir(PASTA_ORIGEM) if f.endswith(".xlsx") and not f.startswith("~$")]

if not arquivos:
    print("⚠️ Nenhum arquivo .xlsx encontrado na pasta de origem.")
else:
    print(f"📂 {len(arquivos)} arquivo(s) encontrados para processamento.\n")

    lista_dfs = []

    for arquivo in arquivos:
        caminho = os.path.join(PASTA_ORIGEM, arquivo)
        print(f"🧾 Processando: {arquivo} ...")

        try:
            df = pd.read_excel(caminho)

            # Verifica colunas obrigatórias
            colunas_obrig = ["Nome da base", "T日签收率-应签收量", "T日签收率-已签收量"]
            if not all(c in df.columns for c in colunas_obrig):
                print(f"❌ Colunas obrigatórias ausentes em {arquivo}. Pulando.\n")
                continue

            # Renomeia colunas
            df = df.rename(columns={
                "T日签收率-应签收量": "Total Recebido",
                "T日签收率-已签收量": "Entregue"
            })

            # Mantém colunas principais
            df = df[["Nome da base", "Total Recebido", "Entregue"]]
            lista_dfs.append(df)

            print(f"✅ {arquivo} processado com sucesso.\n")

        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}\n")

    # ======================================================
    # 📘 Consolida e soma por Nome da base
    # ======================================================
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)

        # Soma os pedidos por Nome da base
        df_resumo = (
            df_consolidado
            .groupby("Nome da base", as_index=False)
            .agg({"Total Recebido": "sum", "Entregue": "sum"})
        )

        # Calcula métricas
        df_resumo["SLA (%)"] = df_resumo.apply(
            lambda x: calcular_sla(x["Entregue"], x["Total Recebido"]), axis=1
        )
        df_resumo["Classificação"] = df_resumo["SLA (%)"].apply(classificar_meta)
        df_resumo["Pontuação Total"] = df_resumo["SLA (%)"].apply(pontuacao_meta)
        df_resumo["Elegibilidade (%)"] = df_resumo["Pontuação Total"] * 100

        # 🔢 Formata o SLA como percentual com 2 casas
        df_resumo["SLA (%)"] = df_resumo["SLA (%)"].map(lambda x: f"{x:.2f}%")

        # Caminho final
        caminho_saida = os.path.join(PASTA_DESTINO, "T0_Resumo_Geral.xlsx")

        # Salva resultado
        df_resumo.to_excel(caminho_saida, index=False)

        print("🎯 Consolidação concluída com sucesso!")
        print(f"💾 Arquivo final salvo em: {caminho_saida}")
        print("\n📊 Colunas: Nome da base | Total Recebido | Entregue | SLA (%) | Classificação | Pontuação Total | Elegibilidade (%)")

    else:
        print("⚠️ Nenhum dado válido encontrado para consolidar.")
