# -*- coding: utf-8 -*-
"""
===========================================================
📦 Consolidação de Bases - Sem Movimentação (com divisão automática)
Versão: 2.4 (2025-10-17)
Autor: bb-assistente 😎
-----------------------------------------------------------
✅ Junta todas as planilhas da pasta
✅ Divide automaticamente em abas de até 1.048.000 linhas
✅ Cria abas "Resumo_Geral" e "Resumo_Arquivos"
✅ Ignora arquivos de saída (Bases_*.xlsx)
===========================================================
"""

import os
import pandas as pd
from tqdm import tqdm

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Sem Movimentação"
ARQUIVO_SAIDA = os.path.join(PASTA_ENTRADA, "Bases_Unificadas.xlsx")
LIMITE_EXCEL = 1_048_000  # limite seguro por aba (~1 milhão de linhas)


# ======================================================
# 🧠 FUNÇÕES AUXILIARES
# ======================================================

def listar_planilhas(pasta: str):
    """
    Lista arquivos Excel válidos, ignorando temporários (~$) e
    arquivos de saída (que começam com 'Bases_').
    """
    arquivos = []
    for f in os.listdir(pasta):
        nome_arquivo = f.lower()
        if (
            nome_arquivo.endswith(".xlsx")
            and not nome_arquivo.startswith("~$")
            and not nome_arquivo.startswith("bases_")  # 👈 ignora Bases_Filtradas, Bases_Unificadas etc.
        ):
            arquivos.append(os.path.join(pasta, f))
    return arquivos


# ======================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ======================================================

def main():
    print(f"🔍 Procurando planilhas em:\n{PASTA_ENTRADA}\n")
    arquivos = listar_planilhas(PASTA_ENTRADA)

    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado.")
        return

    print(f"📁 {len(arquivos)} arquivo(s) encontrado(s):")
    for a in arquivos:
        print(f"  • {os.path.basename(a)}")
    print("")

    dfs = []
    resumo_arquivos = []

    for arquivo in tqdm(arquivos, desc="📖 Lendo planilhas", ncols=80):
        try:
            df = pd.read_excel(arquivo, dtype=str)
            df["Arquivo_Origem"] = os.path.basename(arquivo)
            resumo_arquivos.append({"Arquivo": os.path.basename(arquivo), "Linhas": len(df)})
            dfs.append(df)
        except Exception as e:
            print(f"❌ Erro ao ler '{os.path.basename(arquivo)}': {e}")

    if not dfs:
        print("⚠️ Nenhum dado carregado.")
        return

    # Junta tudo
    df_total = pd.concat(dfs, ignore_index=True, sort=False)
    total_linhas = len(df_total)
    total_colunas = len(df_total.columns)
    print(f"\n📊 Total consolidado: {total_linhas:,} linhas e {total_colunas} colunas\n".replace(",", "."))

    # Dividir o DataFrame em partes menores
    partes = (total_linhas // LIMITE_EXCEL) + 1
    partes_geradas = []

    with pd.ExcelWriter(ARQUIVO_SAIDA, engine='openpyxl') as writer:
        for i in tqdm(range(partes), desc="✂️ Criando abas", ncols=80):
            inicio = i * LIMITE_EXCEL
            fim = (i + 1) * LIMITE_EXCEL
            df_parte = df_total.iloc[inicio:fim]
            if df_parte.empty:
                continue
            aba_nome = f"Parte_{i+1}"
            df_parte.to_excel(writer, sheet_name=aba_nome, index=False)
            partes_geradas.append({"Aba": aba_nome, "Linhas": len(df_parte)})
            print(f"✅ {aba_nome} criada ({len(df_parte):,} linhas)".replace(",", "."))

        # Criar abas de resumo no mesmo writer
        df_resumo = pd.DataFrame(partes_geradas)
        df_arquivos = pd.DataFrame(resumo_arquivos)
        df_resumo.to_excel(writer, sheet_name="Resumo_Geral", index=False)
        df_arquivos.to_excel(writer, sheet_name="Resumo_Arquivos", index=False)

    print("\n✅ Consolidação concluída com sucesso!")
    print(f"📁 Arquivo final salvo em:\n{ARQUIVO_SAIDA}")
    print(f"📊 Total: {total_linhas:,} linhas em {len(partes_geradas)} aba(s)\n".replace(",", "."))


# ======================================================
# ▶️ EXECUTAR
# ======================================================
if __name__ == "__main__":
    main()