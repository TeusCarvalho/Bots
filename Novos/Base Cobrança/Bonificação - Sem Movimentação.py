# -*- coding: utf-8 -*-
"""
📦 Relatório Sem Movimentação — Franquias (com filtros avançados)
Autor: bb-assistente 😎

🧩 Funcionalidades:
- Lê todas as planilhas Excel de uma pasta
- Mantém TODAS as bases
- Renomeia colunas 中文 → português
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
    "Exceed 30 days with no track"
]

# ======================================================
# 🧠 FUNÇÕES AUXILIARES
# ======================================================

def listar_planilhas(pasta: str):
    arquivos = []
    for f in os.listdir(pasta):
        nome = f.lower()
        if nome.endswith(".xlsx") and not nome.startswith("~$") and f != os.path.basename(ARQUIVO_SAIDA):
            arquivos.append(os.path.join(pasta, f))
    return arquivos


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

    dfs_lazy = []
    for arquivo in tqdm(arquivos, desc="📖 Lendo planilhas", ncols=80):
        try:
            df = pl.read_excel(arquivo)

            # ===========================================
            # 🔤 Renomear colunas 中文 → PT-BR
            # ===========================================
            rename_map = {}
            for c in df.columns:
                if "责任所属代理区" in c or c == "Regional responsável":
                    rename_map[c] = "Regional responsável"
                elif "责任机构" in c or c in ("Unidade responsável", "Unidade responsável责任机构"):
                    rename_map[c] = "Nome da base"
                elif "Aging" in c:
                    rename_map[c] = "Aging"
                elif "JMS" in c or "运单号" in c or c == "Número de pedido JMS 运单号":
                    rename_map[c] = "Remessa"
                elif "问题件名称" in c:
                    rename_map[c] = "Nome de pacote problemático问题件名称"
                elif "最新操作类型" in c:
                    rename_map[c] = "Tipo da última operação最新操作类型"

            df = df.rename(rename_map)

            obrig = ["Regional responsável", "Nome da base", "Aging", "Remessa"]
            if not all(c in df.columns for c in obrig):
                print(f"{Fore.YELLOW}⚠️ Planilha ignorada (colunas faltando): {os.path.basename(arquivo)}")
                continue

            df = df.with_columns(pl.lit(os.path.basename(arquivo)).alias("Arquivo_Origem"))
            dfs_lazy.append(df.lazy())

        except Exception as e:
            print(f"{Fore.RED}❌ Erro ao ler '{os.path.basename(arquivo)}': {e}")

    if not dfs_lazy:
        print(f"{Fore.YELLOW}⚠️ Nenhum dado carregado.")
        return

    print(f"{Fore.CYAN}🧩 Unindo todos os arquivos...{Style.RESET_ALL}")
    df_total = pl.concat(dfs_lazy).collect()

    print(f"\n📊 Total de linhas unificadas: {df_total.height:,}\n".replace(",", "."))

    # ======================================================
    # 🚫 Remover status problemáticos
    # ======================================================
    linhas_antes_total = df_total.height

    col_nome_problema = "Nome de pacote problemático问题件名称"
    col_tipo_operacao = "Tipo da última operação最新操作类型"

    if col_nome_problema in df_total.columns:
        antes = df_total.height
        df_total = df_total.filter(pl.col(col_nome_problema) != "Mercadorias.que.chegam.incompletos货未到齐")
        print(f"{Fore.GREEN}🧹 {antes - df_total.height} linha(s) removidas com status 'Mercadorias.que.chegam.incompletos货未到齐'")
    else:
        print(f"{Fore.YELLOW}⚠️ Coluna de problema não encontrada.")

    if col_tipo_operacao in df_total.columns:
        antes = df_total.height
        df_total = df_total.filter(pl.col(col_tipo_operacao) != "发件扫描/Bipe de expedição")
        print(f"{Fore.GREEN}🧹 {antes - df_total.height} linha(s) removidas com status '发件扫描/Bipe de expedição'")
    else:
        print(f"{Fore.YELLOW}⚠️ Coluna de tipo de operação não encontrada.")

    # ======================================================
    # ⏱️ Filtro de Aging
    # ======================================================
    if "Aging" in df_total.columns:
        df_total = df_total.filter(pl.col("Aging").is_in(AGINGS_DESEJADOS))
        print(f"{Fore.CYAN}⏱️ Filtrado por Aging conforme lista de interesse.{Style.RESET_ALL}")
    else:
        print(f"{Fore.YELLOW}⚠️ Coluna 'Aging' não encontrada, sem filtragem aplicada.")

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
    if "Nome da base" in df_total.columns:
        resumo = (
            df_total
            .group_by("Nome da base")
            .len()
            .sort("len", descending=True)
        )

        print(f"{Fore.CYAN}📊 Linhas por base:{Style.RESET_ALL}")
        for row in resumo.iter_rows():
            print(f"  • {row[0]}: {row[1]} linhas")

        # 🔝 Exportar Top 10 bases em aba separada
        top10 = resumo.head(10).to_pandas()
        with pd.ExcelWriter(ARQUIVO_SAIDA, mode="a", engine="openpyxl") as writer:
            top10.to_excel(writer, sheet_name="Top 10 Bases", index=False)
        print(f"\n🏆 {Fore.GREEN}Aba 'Top 10 Bases' adicionada ao relatório!{Style.RESET_ALL}")


# ======================================================
# ▶️ EXECUTAR
# ======================================================
if __name__ == "__main__":
    main()
