# -*- coding: utf-8 -*-
# pip install polars pandas tqdm openpyxl xlsxwriter

import os
from typing import List, Optional

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

COL_NOME_PROBLEMA = "Nome de pacote problemático问题件名称"
COL_TIPO_OPERACAO = "Tipo da última operação最新操作类型"
# ======================================================
# 🧠 FUNÇÕES AUXILIARES
# ======================================================

def listar_planilhas(pasta: str, arquivo_saida: str) -> List[str]:
    """Lista arquivos Excel válidos, ignorando temporários e o arquivo de saída."""
    saida_nome = os.path.basename(arquivo_saida).lower()
    arquivos = []

    for f in os.listdir(pasta):
        nome = f.lower()
        if (
            nome.endswith(".xlsx")
            and not nome.startswith("~$")
            and nome != saida_nome
        ):
            arquivos.append(os.path.join(pasta, f))

    return arquivos


def encontrar_coluna_unidade(df: pl.DataFrame) -> Optional[str]:
    """Tenta localizar a coluna 'Unidade responsável责任机构' mesmo com variações."""
    for col in df.columns:
        nome_up = col.upper()
        if "UNIDADE" in nome_up or "RESPONSÁVEL" in nome_up or "责任机构" in col:
            return col
    return None


def ler_excel_com_origem(path: str) -> Optional[pl.DataFrame]:
    """Lê Excel com Polars e adiciona coluna de origem."""
    try:
        df = pl.read_excel(path)
        df = df.with_columns(
            pl.lit(os.path.basename(path)).alias("Arquivo_Origem")
        )
        return df
    except Exception as e:
        print(f"❌ Erro ao ler '{os.path.basename(path)}': {e}")
        return None


def concat_flex(dfs: List[pl.DataFrame]) -> pl.DataFrame:
    """
    Concatena dataframes mesmo com colunas faltantes.
    1) Tenta diagonal_relaxed (melhor opção, se disponível).
    2) Fallback: alinha colunas manualmente e concatena.
    """
    if not dfs:
        return pl.DataFrame()

    # 1) Melhor cenário: Polars moderno
    try:
        return pl.concat(dfs, how="diagonal_relaxed")
    except TypeError:
        pass
    except Exception:
        pass

    # 2) Fallback manual
    all_cols = []
    seen = set()
    for df in dfs:
        for c in df.columns:
            if c not in seen:
                seen.add(c)
                all_cols.append(c)

    aligned = []
    for df in dfs:
        missing = [c for c in all_cols if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        df = df.select(all_cols)
        aligned.append(df)

    try:
        return pl.concat(aligned)
    except pl.exceptions.InvalidOperationError:
        aligned2 = []
        for d in aligned:
            d2 = d.with_columns([
                pl.col(c).cast(pl.String, strict=False).alias(c)
                for c in all_cols
            ])
            aligned2.append(d2)
        return pl.concat(aligned2)


def remover_status_se_existir(
    df: pl.DataFrame,
    col_name: str,
    status: str
) -> pl.DataFrame:
    """Remove linhas onde col == status, se a coluna existir."""
    if col_name in df.columns:
        linhas_antes = df.height
        df = df.filter(pl.col(col_name) != status)
        removidas = linhas_antes - df.height
        print(f"🧹 {removidas} linha(s) com status '{status}' foram removidas.")
        return df
    else:
        print(f"⚠️ Coluna '{col_name}' não encontrada. Nenhuma filtragem aplicada para '{status}'.")
        return df
# ======================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ======================================================

def main():
    print(f"🔎 Procurando planilhas Excel em:\n{PASTA_ENTRADA}\n")
    arquivos = listar_planilhas(PASTA_ENTRADA, ARQUIVO_SAIDA)

    if not arquivos:
        print("⚠️ Nenhum arquivo .xlsx encontrado.")
        return

    print(f"📁 {len(arquivos)} arquivo(s) encontrado(s):")
    for a in arquivos:
        print(f"  • {os.path.basename(a)}")
    print("")

    dfs = []
    for arquivo in tqdm(arquivos, desc="📖 Lendo planilhas", ncols=80):
        df = ler_excel_com_origem(arquivo)
        if df is not None and df.height > 0:
            dfs.append(df)

    if not dfs:
        print("⚠️ Nenhum dado carregado.")
        return

    print("🧩 Unindo arquivos com concat flexível...")
    df_total = concat_flex(dfs)

    print(f"\n📊 Total de linhas consolidadas: {df_total.height:,}\n".replace(",", "."))

    # Encontrar coluna alvo
    coluna_unidade = encontrar_coluna_unidade(df_total)
    if not coluna_unidade:
        print("❌ Não foi possível encontrar a coluna 'Unidade responsável责任机构'.")
        print(f"Colunas disponíveis: {df_total.columns}")
        return

    print(f"✅ Coluna identificada: '{coluna_unidade}'\n")

    # Mostrar variações de escrita
    try:
        valores_unicos = (
            df_total
            .select(pl.col(coluna_unidade))
            .to_series()
            .drop_nulls()
            .unique()
            .to_list()
        )
    except Exception:
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
    # 🚫 Remover status problemáticos (SEM constantes)
    # ======================================================

    linhas_antes_total = df_filtrado.height

    df_filtrado = remover_status_se_existir(
        df_filtrado,
        COL_NOME_PROBLEMA,
        "Mercadorias.que.chegam.incompletos货未到齐"
    )

    df_filtrado = remover_status_se_existir(
        df_filtrado,
        COL_TIPO_OPERACAO,
        "发件扫描/Bipe de expedição"
    )

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
