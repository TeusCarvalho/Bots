# -*- coding: utf-8 -*-
"""
===========================================================
🚚 Resumo T-0 Semanal com Polars Lazy Mode
Versão: 2.0 (2025-10-17)
Autor: bb-assistente 😎
-----------------------------------------------------------
✅ Lê todas as planilhas T-0 (.xlsx)
✅ Usa Polars Lazy para unir e agrupar (super rápido)
✅ Gera comparação semana atual x anterior
✅ Cria resumo por dia e exporta Excel completo
===========================================================
"""

import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# ===========================================================
# CONFIGURAÇÕES
# ===========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PASTA_T0 = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\T-0"
)
ARQUIVO_BASE = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
)

COL_BASE = "Nome da base"
COL_UF = "UF"
COL_DATA = "Horário de término do prazo de coleta"
COL_RECEBIDO = "T日签收率-应签收量"
COL_ENTREGUE = "T日签收率-已签收量"


# ===========================================================
# FUNÇÕES AUXILIARES
# ===========================================================
def ler_planilhas_t0(pasta: Path) -> pl.LazyFrame:
    arquivos = list(pasta.glob("*.xlsx")) + list(pasta.glob("*.xls"))
    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo encontrado na pasta T-0.")

    dfs_lazy = []
    for arq in arquivos:
        if "Resumo_T0_Semanal_" in arq.name:
            continue
        logging.info(f"📄 Lendo T-0: {arq.name}")
        try:
            df = pl.read_excel(arq).lazy()
            df = df.with_columns(pl.lit(arq.name).alias("_arquivo"))
            dfs_lazy.append(df)
        except Exception as e:
            logging.error(f"❌ Erro ao ler {arq.name}: {e}")

    if not dfs_lazy:
        raise RuntimeError("Nenhuma planilha T-0 válida foi carregada.")
    return pl.concat(dfs_lazy)


def calcular_resumo(df: pl.DataFrame, semana: str) -> tuple[pd.DataFrame, pl.DataFrame]:
    df_semana = df.filter(pl.col("Semana") == semana)
    resumo = (
        df_semana
        .group_by(["Semana", COL_UF])
        .agg([
            pl.col(COL_RECEBIDO).sum().alias("Recebido"),
            pl.col(COL_ENTREGUE).sum().alias("Entregue"),
        ])
        .with_columns((pl.col("Entregue") / pl.col("Recebido") * 100)
                      .round(2)
                      .alias("Taxa de Entrega (%)"))
    )

    total = (
        resumo
        .select([
            pl.lit(semana).alias("Semana"),
            pl.lit("TOTAL BRASIL").alias(COL_UF),
            pl.col("Recebido").sum(),
            pl.col("Entregue").sum()
        ])
        .with_columns((pl.col("Entregue") / pl.col("Recebido") * 100)
                      .round(2)
                      .alias("Taxa de Entrega (%)"))
    )

    resumo_final = pl.concat([resumo, total])
    return resumo_final.to_pandas(), df_semana


def calcular_por_dia(df_semana: pl.DataFrame) -> pd.DataFrame:
    df_semana = df_semana.with_columns(pl.col(COL_DATA).dt.date().alias("Dia"))

    resumo_dia = (
        df_semana
        .group_by([COL_UF, "Dia"])
        .agg([
            pl.col(COL_RECEBIDO).sum().alias("Recebido"),
            pl.col(COL_ENTREGUE).sum().alias("Entregue"),
        ])
        .with_columns((pl.col("Entregue") / pl.col("Recebido") * 100)
                      .round(2)
                      .alias("Taxa de Entrega (%)"))
    )

    total_dia = (
        resumo_dia
        .group_by("Dia")
        .agg([
            pl.col("Recebido").sum(),
            pl.col("Entregue").sum(),
        ])
        .with_columns((pl.col("Entregue") / pl.col("Recebido") * 100)
                      .round(2)
                      .alias("Taxa de Entrega (%)"))
        .with_columns(pl.lit("TOTAL BRASIL").alias(COL_UF))
    )

    return pl.concat([resumo_dia, total_dia]).to_pandas()


# ===========================================================
# EXECUÇÃO PRINCIPAL
# ===========================================================
def executar_t0():
    hoje = datetime.now()

    # 🧩 Lê T-0 (Lazy)
    df_dados = ler_planilhas_t0(PASTA_T0).collect()

    # 🧭 Base Coordenadores (pandas é OK aqui)
    logging.info("📂 Lendo planilha base de coordenadores...")
    df_base = pd.read_excel(ARQUIVO_BASE)

    if COL_UF not in df_base.columns:
        df_base[COL_UF] = "UF não encontrado"

    # 🧹 Normalização
    df_base[COL_BASE] = df_base[COL_BASE].astype(str).str.strip().str.upper()
    df_dados = df_dados.with_columns(
        pl.col(COL_BASE).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    )

    # 🧬 Merge (Polars Join)
    df_merge = df_dados.join(
        pl.from_pandas(df_base[[COL_BASE, COL_UF]]),
        on=COL_BASE,
        how="left"
    )

    df_merge = df_merge.with_columns([
        pl.col(COL_UF).fill_null("UF não encontrado"),
        pl.col(COL_DATA).str.strptime(pl.Datetime, strict=False).alias(COL_DATA),
    ])

    # ⚙️ Adiciona semana ISO
    df_merge = df_merge.with_columns(
        ("W" + pl.col(COL_DATA).dt.week().cast(pl.Utf8)).alias("Semana")
    )

    # Descobrir semanas disponíveis
    semanas = sorted(df_merge["Semana"].drop_nans().unique().to_list())
    semana_atual = semanas[-1]
    semana_anterior = semanas[-2] if len(semanas) > 1 else None

    logging.info(f"📅 Semana Atual: {semana_atual}")
    if semana_anterior:
        logging.info(f"📅 Semana Anterior: {semana_anterior}")

    # 🧾 Calcula resumos
    resumo_atual, df_semana_atual = calcular_resumo(df_merge, semana_atual)

    resumo_anterior, comparacao = None, None
    if semana_anterior:
        resumo_anterior, df_semana_anterior = calcular_resumo(df_merge, semana_anterior)
        comp = (
            pl.from_pandas(resumo_atual)
            .join(pl.from_pandas(resumo_anterior), on=COL_UF, suffix="_Anterior")
            .with_columns([
                (pl.col("Recebido") - pl.col("Recebido_Anterior")).alias("Dif_Recebido"),
                (pl.col("Entregue") - pl.col("Entregue_Anterior")).alias("Dif_Entregue"),
                (pl.col("Taxa de Entrega (%)") - pl.col("Taxa de Entrega (%)_Anterior")).alias("Dif_Taxa (%)"),
            ])
        )
        comparacao = comp.to_pandas()

    por_dia = calcular_por_dia(df_semana_atual)

    # 🧾 Exporta Excel
    nome_saida = PASTA_T0 / f"Resumo_T0_Semanal_{hoje.strftime('%Y-%m-%d')}.xlsx"
    with pd.ExcelWriter(nome_saida, engine="openpyxl") as writer:
        resumo_atual.to_excel(writer, index=False, sheet_name=f"{semana_atual}")
        if semana_anterior:
            resumo_anterior.to_excel(writer, index=False, sheet_name=f"{semana_anterior}")
        if comparacao is not None:
            comparacao.to_excel(writer, index=False, sheet_name="Comparação")
        por_dia.to_excel(writer, index=False, sheet_name="Por Dia")

    logging.info(f"✅ Resumo T-0 gerado com sucesso: {nome_saida}")


# ===========================================================
# ▶️ EXECUTAR
# ===========================================================
if __name__ == "__main__":
    executar_t0()
