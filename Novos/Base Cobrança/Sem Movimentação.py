# -*- coding: utf-8 -*-

import os
import polars as pl
import pandas as pd
from tqdm import tqdm

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Sem Movimentação"
ARQUIVO_SAIDA = os.path.join(PASTA_ENTRADA, "Bases_Filtradas.xlsx")

BASES_ALVO = [
    "CZS -AC", "SMD -AC", "TAR -AC", "F BSL-AC",
    "ANA FLUVIAL - PA", "BRV -PA", "MCP FLUVIAL -AP",
    "F PVH-RO", "F MCP-AP", "F MCP 02-AP", "STM FLUVIAL -PA", "ITT -PA",
    "MAO FLUVIAL -AM"
]

# ======================================================
# 🧠 FUNÇÕES AUXILIARES
# ======================================================

def listar_planilhas(pasta: str):
    """Lista arquivos Excel válidos, ignorando temporários e o arquivo de saída."""
    arquivos = []
    for f in os.listdir(pasta):
        nome = f.lower()
        if nome.endswith(".xlsx") and not nome.startswith("~$") and f != os.path.basename(ARQUIVO_SAIDA):
            arquivos.append(os.path.join(pasta, f))
    return arquivos


def encontrar_coluna_unidade(df):
    """Tenta localizar a coluna 'Unidade responsável责任机构' mesmo com variações."""
    for col in df.columns:
        nome = col.upper()
        if "UNIDADE" in nome or "RESPONSÁVEL" in nome or "责任机构" in col:
            return col
    return None


# ======================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ======================================================

def main():
    print(f"🔎 Procurando planilhas Excel em:\n{PASTA_ENTRADA}\n")
    arquivos = listar_planilhas(PASTA_ENTRADA)

    if not arquivos:
        print("⚠️ Nenhum arquivo .xlsx encontrado.")
        return

    print(f"📁 {len(arquivos)} arquivo(s) encontrado(s):")
    for a in arquivos:
        print(f"  • {os.path.basename(a)}")
    print("")

    dfs_lazy = []
    for arquivo in tqdm(arquivos, desc="📖 Lendo planilhas", ncols=80):
        try:
            df_lazy = pl.read_excel(arquivo).lazy()
            df_lazy = df_lazy.with_columns(pl.lit(os.path.basename(arquivo)).alias("Arquivo_Origem"))
            dfs_lazy.append(df_lazy)
        except Exception as e:
            print(f"❌ Erro ao ler '{os.path.basename(arquivo)}': {e}")

    if not dfs_lazy:
        print("⚠️ Nenhum dado carregado.")
        return

    print("🧩 Unindo arquivos com Polars Lazy...")
    df_total = pl.concat(dfs_lazy).collect()
    print(f"\n📊 Total de linhas consolidadas: {df_total.height:,}\n".replace(",", "."))

    # Encontrar coluna alvo
    coluna_unidade = encontrar_coluna_unidade(df_total)
    if not coluna_unidade:
        print("❌ Não foi possível encontrar a coluna 'Unidade responsável责任机构'.")
        print(f"Colunas disponíveis: {df_total.columns}")
        return

    print(f"✅ Coluna identificada: '{coluna_unidade}'\n")

    # Mostrar variações de escrita
    valores_unicos = df_total[coluna_unidade].drop_nulls().unique().to_list()
    print("🔎 Variações de escrita encontradas:\n")
    for v in sorted(valores_unicos):
        print(f"  • {v}")

    # Diferenças em relação às bases oficiais
    print("\n⚠️ Diferenças detectadas (não estão na lista oficial):\n")
    diferentes = [v for v in valores_unicos if v not in BASES_ALVO]
    if diferentes:
        for val in diferentes:
            print(f"  🚫 {val}")
    else:
        print("✅ Nenhuma diferença encontrada! Todas as bases estão corretas.")

    # Filtrar apenas bases oficiais
    df_filtrado = df_total.filter(pl.col(coluna_unidade).is_in(BASES_ALVO))

    # ======================================================
    # 🚫 Remover status problemáticos
    # ======================================================
    col_nome_problema = "Nome de pacote problemático问题件名称"
    col_tipo_operacao = "Tipo da última operação最新操作类型"

    linhas_antes_total = df_filtrado.height

    # 1️⃣ Remover "Mercadorias.que.chegam.incompletos货未到齐"
    if col_nome_problema in df_filtrado.columns:
        linhas_antes = df_filtrado.height
        df_filtrado = df_filtrado.filter(
            pl.col(col_nome_problema) != "Mercadorias.que.chegam.incompletos货未到齐"
        )
        removidas = linhas_antes - df_filtrado.height
        print(f"🧹 {removidas} linha(s) com status 'Mercadorias.que.chegam.incompletos货未到齐' foram removidas.")
    else:
        print(
            "⚠️ Coluna 'Nome de pacote problemático问题件名称' não encontrada. Nenhuma filtragem aplicada para esse status.")

    # 2️⃣ Remover "发件扫描/Bipe de expedição"
    if col_tipo_operacao in df_filtrado.columns:
        linhas_antes = df_filtrado.height
        df_filtrado = df_filtrado.filter(
            pl.col(col_tipo_operacao) != "发件扫描/Bipe de expedição"
        )
        removidas = linhas_antes - df_filtrado.height
        print(f"🧹 {removidas} linha(s) com status '发件扫描/Bipe de expedição' foram removidas.")
    else:
        print(
            "⚠️ Coluna 'Tipo da última operação最新操作类型' não encontrada. Nenhuma filtragem aplicada para esse status.")

    linhas_depois_total = df_filtrado.height
    total_removidas = linhas_antes_total - linhas_depois_total

    print(f"\n🧾 Total de {total_removidas} linha(s) removidas no total.\n")

    # ======================================================
    # 🧮 Verificação final
    # ======================================================
    if df_filtrado.is_empty():
        print("\n⚠️ Nenhuma linha correspondente às bases desejadas foi encontrada.")
        return

    # Converter para pandas e exportar
    df_final = df_filtrado.to_pandas()
    df_final.to_excel(ARQUIVO_SAIDA, index=False)

    resumo = df_final[coluna_unidade].value_counts()
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
