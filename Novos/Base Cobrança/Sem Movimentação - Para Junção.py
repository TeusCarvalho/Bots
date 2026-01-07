# -*- coding: utf-8 -*-

import os
import math
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
            and f.lower() != os.path.basename(ARQUIVO_SAIDA).lower()
        ):
            arquivos.append(os.path.join(pasta, f))
    return arquivos


def harmonizar_sinonimos(df: pl.DataFrame) -> pl.DataFrame:
    """
    Une colunas que são a mesma informação com nomes diferentes.
    Regra: cria/atualiza a coluna canônica com COALESCE(candidatas...) e remove as candidatas extras.
    """

    # ✅ Ajuste aqui conforme você encontrar novos casos
    SINONIMOS = {
        "Remessa": ["Remessa", "Número de pedido JMS 运单号"],
    }

    for canonica, candidatas in SINONIMOS.items():
        existentes = [c for c in candidatas if c in df.columns]
        if not existentes:
            continue

        if canonica in df.columns:
            # canonica já existe: coalesce(canonica, outras...)
            cols = [pl.col(canonica)] + [pl.col(c) for c in existentes if c != canonica]
            df = df.with_columns(pl.coalesce(cols).alias(canonica))
        else:
            # canonica não existe: cria a partir das candidatas
            cols = [pl.col(c) for c in existentes]
            df = df.with_columns(pl.coalesce(cols).alias(canonica))

        # remove candidatas extras (mantém apenas a canônica)
        drop_cols = [c for c in existentes if c != canonica]
        if drop_cols:
            df = df.drop(drop_cols)

    return df
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

    for arquivo in tqdm(arquivos, desc="📖 Lendo planilhas", ncols=80):
        nome = os.path.basename(arquivo)
        try:
            # Lê UMA vez (antes você lia 2x por arquivo)
            df_eager = pl.read_excel(arquivo)

            # Harmoniza nomes equivalentes (ex.: 运单号 -> Remessa)
            df_eager = harmonizar_sinonimos(df_eager)

            # Adiciona coluna de origem
            df_eager = df_eager.with_columns(pl.lit(nome).alias("Arquivo_Origem"))

            # Lazy para permitir concat com otimizações
            dfs_lazy.append(df_eager.lazy())

            resumo_arquivos.append({"Arquivo": nome, "Linhas": df_eager.height})

        except Exception as e:
            print(f"❌ Erro ao ler '{nome}': {e}")

    if not dfs_lazy:
        print("⚠️ Nenhum dado carregado.")
        return

    print("🧩 Combinando tudo com Polars (concat seguro)...")

    # ✅ Importante:
    # how="diagonal_relaxed" une colunas diferentes e preenche faltantes com null,
    # além de tentar compatibilizar tipos quando necessário.
    df_total = pl.concat(dfs_lazy, how="diagonal_relaxed").collect()

    total_linhas, total_colunas = df_total.shape
    print(f"\n📊 Total consolidado: {total_linhas:,} linhas e {total_colunas} colunas\n".replace(",", "."))
    # Divide em partes para respeitar limite do Excel
    partes = math.ceil(total_linhas / LIMITE_EXCEL)
    partes_geradas = []

    with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
        for i in tqdm(range(partes), desc="✂️ Criando abas", ncols=80):
            inicio = i * LIMITE_EXCEL
            if inicio >= total_linhas:
                break

            qtd = min(LIMITE_EXCEL, total_linhas - inicio)

            # ✅ Converte apenas o pedaço necessário para Pandas (melhor RAM)
            df_parte_pd = df_total.slice(inicio, qtd).to_pandas()

            if df_parte_pd.empty:
                continue

            aba_nome = f"Parte_{i+1}"
            df_parte_pd.to_excel(writer, sheet_name=aba_nome, index=False)

            partes_geradas.append({"Aba": aba_nome, "Linhas": len(df_parte_pd)})
            print(f"✅ {aba_nome} criada ({len(df_parte_pd):,} linhas)".replace(",", "."))

        # Abas de resumo
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
