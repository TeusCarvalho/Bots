# -*- coding: utf-8 -*-

import os
import polars as pl
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
    """Lista arquivos Excel válidos, ignorando temporários (~$) e saídas."""
    arquivos = []
    for f in os.listdir(pasta):
        nome_arquivo = f.lower()
        if (
            nome_arquivo.endswith(".xlsx")
            and not nome_arquivo.startswith("~$")
            and not nome_arquivo.startswith("bases_")
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

    dfs_lazy = []
    resumo_arquivos = []

    # Lê cada planilha com Polars LazyFrame (sem carregar tudo na memória)
    for arquivo in tqdm(arquivos, desc="📖 Lendo planilhas", ncols=80):
        try:
            df = pl.read_excel(arquivo).lazy()
            nome = os.path.basename(arquivo)
            # Adiciona coluna de origem
            df = df.with_columns(pl.lit(nome).alias("Arquivo_Origem"))
            dfs_lazy.append(df)

            # Conta rápido o total de linhas do arquivo
            linhas = pl.read_excel(arquivo).height
            resumo_arquivos.append({"Arquivo": nome, "Linhas": linhas})
        except Exception as e:
            print(f"❌ Erro ao ler '{os.path.basename(arquivo)}': {e}")

    if not dfs_lazy:
        print("⚠️ Nenhum dado carregado.")
        return

    print("🧩 Combinando tudo com Polars (Lazy)...")
    df_total = pl.concat(dfs_lazy).collect()  # executa a computação
    total_linhas, total_colunas = df_total.shape
    print(f"\n📊 Total consolidado: {total_linhas:,} linhas e {total_colunas} colunas\n".replace(",", "."))

    # Converter para pandas para salvar em Excel
    df_total_pd = df_total.to_pandas()

    # Dividir o DataFrame em partes menores
    partes = (total_linhas // LIMITE_EXCEL) + 1
    partes_geradas = []

    with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
        for i in tqdm(range(partes), desc="✂️ Criando abas", ncols=80):
            inicio = i * LIMITE_EXCEL
            fim = (i + 1) * LIMITE_EXCEL
            df_parte = df_total_pd.iloc[inicio:fim]
            if df_parte.empty:
                continue
            aba_nome = f"Parte_{i+1}"
            df_parte.to_excel(writer, sheet_name=aba_nome, index=False)
            partes_geradas.append({"Aba": aba_nome, "Linhas": len(df_parte)})
            print(f"✅ {aba_nome} criada ({len(df_parte):,} linhas)".replace(",", "."))

        # Criar abas de resumo no mesmo arquivo
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