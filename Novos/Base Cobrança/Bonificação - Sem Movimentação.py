# -*- coding: utf-8 -*-
"""
📦 Relatório Sem Movimentação — Franquias (com filtros avançados)
Autor: bb-assistente 😎

🧩 Funcionalidades:
- Lê todas as planilhas Excel de uma pasta
- Mantém TODAS as bases e TODAS as colunas (mesmo quando alguns arquivos têm colunas extras)
- Renomeia colunas 中文 → português (colunas-chave)
- Filtra pelos status de aging especificados
- Remove status problemáticos ("Mercadorias.que.chegam.incompletos货未到齐" e "发件扫描/Bipe de expedição")
- Gera relatório consolidado em Excel
- Adiciona aba 'Top 10 Bases'
"""

import os
import polars as pl
import pandas as pd
from tqdm import tqdm
from colorama import Fore, Style, init

init(autoreset=True)

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Sem Movimentação"
ARQUIVO_SAIDA = os.path.join(PASTA_ENTRADA, "Relatorio_Sem_Movimentacao.xlsx")

# Filtros de aging
AGINGS_DESEJADOS = [
    "Exceed 5 days with no track",
    "Exceed 6 days with no track",
    "Exceed 7 days with no track",
    "Exceed 10 days with no track",
    "Exceed 14 days with no track",
    "Exceed 30 days with no track",
]

# Status problemáticos a remover
STATUS_PROBLEMA_1 = "Mercadorias.que.chegam.incompletos货未到齐"
STATUS_PROBLEMA_2 = "发件扫描/Bipe de expedição"

# Colunas esperadas (após renome)
COL_REGIONAL_RESP = "Regional responsável"
COL_BASE = "Nome da base"
COL_AGING = "Aging"
COL_REMESSA = "Remessa"
COL_PROBLEMA = "Nome de pacote problemático问题件名称"
COL_TIPO_OP = "Tipo da última operação最新操作类型"


# ======================================================
# 🧠 FUNÇÕES AUXILIARES
# ======================================================

def listar_planilhas(pasta: str):
    arquivos = []
    saida_nome = os.path.basename(ARQUIVO_SAIDA)
    for f in os.listdir(pasta):
        nome = f.lower()
        if nome.endswith(".xlsx") and not nome.startswith("~$") and f != saida_nome:
            arquivos.append(os.path.join(pasta, f))
    return arquivos


def normalizar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    # evita diferenças bobas (espaços, quebras de linha) virarem "colunas diferentes"
    rename = {}
    for c in df.columns:
        c2 = str(c).replace("\n", " ").strip()
        if c2 != c:
            rename[c] = c2
    return df.rename(rename) if rename else df


def renomear_colunas_chave(df: pl.DataFrame) -> pl.DataFrame:
    """
    Renomeia só o que é chave para filtros/relatório.
    O resto mantém como veio (para não perder nada).
    """
    rename_map = {}
    for c in df.columns:
        c_str = str(c)

        if "责任所属代理区" in c_str or c_str == "Regional responsável":
            rename_map[c] = COL_REGIONAL_RESP

        elif "责任机构" in c_str or c_str in ("Unidade responsável", "Unidade responsável责任机构"):
            rename_map[c] = COL_BASE

        elif "Aging" in c_str:
            rename_map[c] = COL_AGING

        elif ("JMS" in c_str) or ("运单号" in c_str) or (c_str == "Número de pedido JMS 运单号"):
            rename_map[c] = COL_REMESSA

        elif "问题件名称" in c_str:
            rename_map[c] = COL_PROBLEMA

        elif "最新操作类型" in c_str:
            rename_map[c] = COL_TIPO_OP

    return df.rename(rename_map) if rename_map else df


def castar_colunas_chave(df: pl.DataFrame) -> pl.DataFrame:
    """
    Padroniza tipos nas colunas-chave para evitar erro de concat por dtype diferente entre arquivos.
    """
    cols = [COL_REGIONAL_RESP, COL_BASE, COL_AGING, COL_REMESSA, COL_PROBLEMA, COL_TIPO_OP, "Arquivo_Origem"]
    exprs = []
    for c in cols:
        if c in df.columns:
            exprs.append(pl.col(c).cast(pl.Utf8))
    return df.with_columns(exprs) if exprs else df


# ======================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ======================================================

def main():
    print(f"{Fore.CYAN}🔎 Procurando planilhas Excel em:\n{Style.RESET_ALL}{PASTA_ENTRADA}\n")
    arquivos = listar_planilhas(PASTA_ENTRADA)

    if not arquivos:
        print(f"{Fore.YELLOW}⚠️ Nenhum arquivo .xlsx encontrado.")
        return

    print(f"{Fore.CYAN}📁 {len(arquivos)} arquivo(s) encontrado(s):{Style.RESET_ALL}")
    for a in arquivos:
        print(f"  • {os.path.basename(a)}")
    print("")

    dfs = []

    for arquivo in tqdm(arquivos, desc="📖 Lendo planilhas", ncols=80):
        try:
            df = pl.read_excel(arquivo)
            df = normalizar_colunas(df)

            # 🔤 Renomear colunas-chave 中文 → PT-BR
            df = renomear_colunas_chave(df)

            # Checar obrigatórias
            obrig = [COL_REGIONAL_RESP, COL_BASE, COL_AGING, COL_REMESSA]
            if not all(c in df.columns for c in obrig):
                faltando = [c for c in obrig if c not in df.columns]
                print(f"{Fore.YELLOW}⚠️ Planilha ignorada (faltando {faltando}): {os.path.basename(arquivo)}")
                continue

            # Origem
            df = df.with_columns(pl.lit(os.path.basename(arquivo)).alias("Arquivo_Origem"))

            # Padroniza tipos nas colunas-chave (evita dtype mismatch)
            df = castar_colunas_chave(df)

            dfs.append(df)

        except Exception as e:
            print(f"{Fore.RED}❌ Erro ao ler '{os.path.basename(arquivo)}': {e}")

    if not dfs:
        print(f"{Fore.YELLOW}⚠️ Nenhum dado carregado.")
        return

    print(f"{Fore.CYAN}🧩 Unindo todos os arquivos (aceitando colunas extras)...{Style.RESET_ALL}")

    # ✅ CORREÇÃO PRINCIPAL:
    # - diagonal_relaxed = faz união das colunas (mantém colunas extras como "Nome da Estação")
    #   e tenta relaxar tipos se houver pequenas diferenças entre arquivos.
    try:
        df_total = pl.concat(dfs, how="diagonal_relaxed", rechunk=True)
    except TypeError:
        # fallback caso sua versão do Polars não tenha diagonal_relaxed
        df_total = pl.concat(dfs, how="diagonal", rechunk=True)

    print(f"\n📊 Total de linhas unificadas: {df_total.height:,}\n".replace(",", "."))

    # ======================================================
    # 🚫 Remover status problemáticos
    # ======================================================
    linhas_antes_total = df_total.height

    if COL_PROBLEMA in df_total.columns:
        antes = df_total.height
        df_total = df_total.filter(pl.col(COL_PROBLEMA) != STATUS_PROBLEMA_1)
        print(f"{Fore.GREEN}🧹 {antes - df_total.height} linha(s) removidas com status '{STATUS_PROBLEMA_1}'")
    else:
        print(f"{Fore.YELLOW}⚠️ Coluna de problema não encontrada ({COL_PROBLEMA}).")

    if COL_TIPO_OP in df_total.columns:
        antes = df_total.height
        df_total = df_total.filter(pl.col(COL_TIPO_OP) != STATUS_PROBLEMA_2)
        print(f"{Fore.GREEN}🧹 {antes - df_total.height} linha(s) removidas com status '{STATUS_PROBLEMA_2}'")
    else:
        print(f"{Fore.YELLOW}⚠️ Coluna de tipo de operação não encontrada ({COL_TIPO_OP}).")

    # ======================================================
    # ⏱️ Filtro de Aging
    # ======================================================
    if COL_AGING in df_total.columns:
        df_total = df_total.filter(pl.col(COL_AGING).is_in(AGINGS_DESEJADOS))
        print(f"{Fore.CYAN}⏱️ Filtrado por Aging conforme lista de interesse.{Style.RESET_ALL}")
    else:
        print(f"{Fore.YELLOW}⚠️ Coluna '{COL_AGING}' não encontrada, sem filtragem aplicada.")

    linhas_depois_total = df_total.height
    print(f"\n🧾 Total de {linhas_antes_total - linhas_depois_total} linha(s) removidas no total.\n")

    if df_total.is_empty():
        print(f"{Fore.YELLOW}⚠️ Nenhum registro restante após filtragem.")
        return

    # ======================================================
    # 💾 Exportar Excel principal
    # ======================================================
    df_total.write_excel(ARQUIVO_SAIDA)
    print(f"{Fore.GREEN}✅ Relatório final gerado com sucesso!")
    print(f"📁 Local: {ARQUIVO_SAIDA}{Style.RESET_ALL}\n")

    # ======================================================
    # 📊 Resumo final + Top 10 Bases
    # ======================================================
    if COL_BASE in df_total.columns:
        resumo = (
            df_total
            .group_by(COL_BASE)
            .len()
            .sort("len", descending=True)
        )

        print(f"{Fore.CYAN}📊 Linhas por base:{Style.RESET_ALL}")
        for row in resumo.iter_rows():
            print(f"  • {row[0]}: {row[1]} linhas")

        # 🔝 Exportar Top 10 bases em aba separada
        top10 = resumo.head(10).to_pandas()
        with pd.ExcelWriter(ARQUIVO_SAIDA, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
            top10.to_excel(writer, sheet_name="Top 10 Bases", index=False)

        print(f"\n🏆 {Fore.GREEN}Aba 'Top 10 Bases' adicionada/atualizada no relatório!{Style.RESET_ALL}")


# ======================================================
# ▶️ EXECUTAR
# ======================================================
if __name__ == "__main__":
    main()