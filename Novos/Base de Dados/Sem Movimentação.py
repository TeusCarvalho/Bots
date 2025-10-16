# -*- coding: utf-8 -*-
"""
===========================================================
📦 Consolidação e Verificação de Bases - Sem Movimentação
Versão: 1.6 (2025-10-16)
Autor: bb-assistente 😎
-----------------------------------------------------------
✅ Lê todas as planilhas .xlsx da pasta
✅ Ignora o arquivo de saída (Bases_Filtradas.xlsx)
✅ Localiza a coluna 'Unidade responsável责任机构'
✅ Mostra variações de escrita e filtra as bases desejadas
===========================================================
"""

import os
import pandas as pd
from tqdm import tqdm

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Sem Movimentação"
ARQUIVO_SAIDA = os.path.join(PASTA_ENTRADA, "Bases_Filtradas.xlsx")

# Bases oficiais (as corretas)
BASES_ALVO = [
    "CZS -AC",
    "SMD -AC",
    "TAR -AC",
    "F BSL-AC",
    "ANA FLUVIAL - PA",
    "BRV -PA",
    "MCP FLUVIAL -AP",
    "F PVH-RO",
    "F MCP-AP",
    "F MCP 02-AP"
]

# ======================================================
# 🧠 FUNÇÕES AUXILIARES
# ======================================================

def listar_planilhas(pasta: str):
    """Retorna todos os arquivos .xlsx da pasta, exceto o arquivo de saída."""
    arquivos = []
    for f in os.listdir(pasta):
        if f.lower().endswith(".xlsx") and not f.lower().startswith("~$") and f != os.path.basename(ARQUIVO_SAIDA):
            arquivos.append(os.path.join(pasta, f))
    return arquivos


def encontrar_coluna_unidade(df):
    """Tenta localizar a coluna de Unidade Responsável mesmo com variações de nome."""
    for col in df.columns:
        if "UNIDADE" in col.upper() or "RESPONSÁVEL" in col.upper() or "责任机构" in col:
            return col
    return None


# ======================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ======================================================

def main():
    print(f"🔎 Procurando planilhas Excel em:\n{PASTA_ENTRADA}\n")
    arquivos = listar_planilhas(PASTA_ENTRADA)

    if not arquivos:
        print("⚠️ Nenhum arquivo .xlsx encontrado nessa pasta.")
        return

    print(f"📁 {len(arquivos)} arquivo(s) encontrado(s):")
    for arq in arquivos:
        print(f"  • {os.path.basename(arq)}")
    print("")

    dfs = []
    for arquivo in tqdm(arquivos, desc="Lendo planilhas", ncols=80):
        try:
            df = pd.read_excel(arquivo)
            df["Arquivo_Origem"] = os.path.basename(arquivo)
            dfs.append(df)
        except Exception as e:
            print(f"❌ Erro ao ler '{os.path.basename(arquivo)}': {e}")

    if not dfs:
        print("⚠️ Nenhum dado foi carregado.")
        return

    # Junta todas as planilhas
    df_total = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 Total de linhas consolidadas: {len(df_total)}\n")

    # Localiza a coluna certa
    coluna_unidade = encontrar_coluna_unidade(df_total)
    if not coluna_unidade:
        print("❌ Não foi possível encontrar a coluna 'Unidade responsável责任机构'.")
        print(f"Colunas disponíveis: {list(df_total.columns)}")
        return

    print(f"✅ Coluna encontrada: '{coluna_unidade}'\n")

    # Exibir variações encontradas
    print("🔎 Variações de escrita encontradas:\n")
    valores_unicos = df_total[coluna_unidade].dropna().unique()
    for valor in sorted(valores_unicos):
        print(f"  • {valor}")

    # Mostrar diferenças
    print("\n⚠️ Diferenças detectadas (não estão na lista oficial):\n")
    diferentes = [v for v in valores_unicos if v not in BASES_ALVO]
    if diferentes:
        for val in diferentes:
            print(f"  🚫 {val}")
    else:
        print("✅ Nenhuma diferença encontrada! Todas as bases estão escritas corretamente.")

    # Filtrar apenas bases oficiais
    df_filtrado = df_total[df_total[coluna_unidade].isin(BASES_ALVO)].copy()

    if df_filtrado.empty:
        print("\n⚠️ Nenhuma linha correspondente às bases desejadas foi encontrada.")
        return

    # Gera Excel consolidado
    df_filtrado.to_excel(ARQUIVO_SAIDA, index=False)

    resumo = df_filtrado[coluna_unidade].value_counts()
    print("\n📊 Resumo das bases filtradas:")
    for base, qtd in resumo.items():
        print(f"  • {base}: {qtd} linhas")

    print(f"\n✅ Planilha consolidada gerada com sucesso!")
    print(f"📁 Local: {ARQUIVO_SAIDA}\n")


# ======================================================
# ▶️ EXECUTAR
# ======================================================
if __name__ == "__main__":
    main()