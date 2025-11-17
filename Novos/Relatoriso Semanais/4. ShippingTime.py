# -*- coding: utf-8 -*-
"""
📦 Comparativo Shipping Time Semanal — FINAL v2.4
---------------------------------------------------------------
- Corrige tipos numéricos automaticamente
- Usa PDD de Entrega como base
- Calcula Etapas 6, 7, 8 e Tempo Total
- Gera comparativo limpo e compatível
- Mantém apenas UFs especificadas
- ✨ MOSTRA TOP 5 ofensores (Etapas 6, 7 e 8) no terminal
- Adiciona linha TOTAL GERAL
- Cria abas separadas por Data automaticamente
- Inclui aba(s) 'Base Consolidada' com junção total
- Divide automaticamente se ultrapassar 1.048.000 linhas
- ✨ Exibe resumo executivo no terminal (maior piora/melhora)
- ✨ Exibe resumo diário (por Data) das Etapas 6/7/8 no terminal
"""

import polars as pl
import pandas as pd
import os
import glob
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")

# =================== CONFIG ===================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\1. Shipping Time"
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

UFS_PERMITIDAS = ["PA", "MT", "GO", "AM", "MS", "RO", "TO", "DF", "RR", "AC", "AP"]
LIMITE_EXCEL = 1_048_000  # limite de linhas por aba

# --- Nomes das Colunas-Chave (para fácil manutenção) ---
COL_PDD_ENTREGA = "PDD de Entrega"
COL_ESTADO_ENTREGA = "Estado de Entrega"
COL_DATA = "Data"
COL_ETAPA_6 = "Tempo trânsito SC Destino->Base Entrega"
COL_ETAPA_7 = "Tempo médio processamento Base Entrega"
COL_ETAPA_8 = "Tempo médio Saída para Entrega->Entrega"
COL_TEMPO_TOTAL = "Tempo Total (h)"

# =================== FUNÇÕES ===================

def encontrar_duas_ultimas_pastas(base_path):
    pastas = [
        os.path.join(base_path, d)
        for d in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, d)) and "output" not in d.lower()
    ]
    pastas.sort(key=os.path.getmtime, reverse=True)
    return pastas[:2] if len(pastas) >= 2 else []


def ler_todos_excel(pasta):
    arquivos = [
        arq for arq in glob.glob(os.path.join(pasta, "*.xls*"))
        if not os.path.basename(arq).startswith("~$")
    ]
    if not arquivos:
        print(f"⚠️ Nenhum arquivo Excel encontrado em: {pasta}")
        return None

    print(f"\n📂 Lendo planilhas da pasta: {os.path.basename(pasta)}")
    dfs = []
    for arq in tqdm(arquivos, desc="📊 Processando arquivos", unit="arquivo"):
        try:
            df = pl.read_excel(arq)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Erro ao ler {os.path.basename(arq)}: {e}")

    if not dfs:
        return None

    df_final = pl.concat(dfs, how="vertical")
    df_final = df_final.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in df_final.columns])
    return df_final


def filtrar_por_uf(df):
    if COL_ESTADO_ENTREGA not in df.columns:
        print(f"⚠️ Coluna '{COL_ESTADO_ENTREGA}' não encontrada — mantendo todas as linhas.")
        return df
    df = df.filter(pl.col(COL_ESTADO_ENTREGA).is_in(UFS_PERMITIDAS))
    print(f"✅ Linhas mantidas apenas das UFs: {', '.join(UFS_PERMITIDAS)}")
    return df


def limpar_coluna_num(df, col):
    return (
        df[col]
        .str.replace_all(r"[^\d,.\-]", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
        .fill_null(0)
        .fill_nan(0)
    )


def calcular_tempo_medio(df):
    # Garante que as colunas das etapas existem
    for col in [COL_ETAPA_6, COL_ETAPA_7, COL_ETAPA_8]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    df = df.with_columns([
        limpar_coluna_num(df, COL_ETAPA_6).alias(COL_ETAPA_6),
        limpar_coluna_num(df, COL_ETAPA_7).alias(COL_ETAPA_7),
        limpar_coluna_num(df, COL_ETAPA_8).alias(COL_ETAPA_8)
    ])

    df = df.with_columns([
        (pl.col(COL_ETAPA_6) + pl.col(COL_ETAPA_7) + pl.col(COL_ETAPA_8)).alias(COL_TEMPO_TOTAL)
    ])

    agrupado = (
        df.group_by(COL_PDD_ENTREGA)
        .agg([
            pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
            pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
            pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
            pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
        ])
        .rename({COL_PDD_ENTREGA: "Base Entrega"})
    )

    total_geral = (
        agrupado.select([
            pl.lit("TOTAL GERAL").alias("Base Entrega"),
            pl.col("Etapa 6 (h)").mean(),
            pl.col("Etapa 7 (h)").mean(),
            pl.col("Etapa 8 (h)").mean(),
            pl.col("Tempo Total (h)").mean()
        ])
    )
    agrupado = pl.concat([agrupado, total_geral], how="vertical")

    return agrupado, df


def gerar_comparativo(semana_ant, semana_atual):
    comp = semana_ant.join(semana_atual, on="Base Entrega", how="outer", suffix="_Atual")
    for etapa in ["Etapa 6", "Etapa 7", "Etapa 8", "Tempo Total"]:
        comp = comp.with_columns([
            pl.col(f"{etapa} (h)").cast(pl.Float64, strict=False).alias(f"{etapa} (h)"),
            pl.col(f"{etapa} (h)_Atual").cast(pl.Float64, strict=False).alias(f"{etapa} (h)_Atual")
        ])
        comp = comp.with_columns([
            (pl.col(f"{etapa} (h)_Atual") - pl.col(f"{etapa} (h)")).alias(f"{etapa} Δ (h)")
        ])
    return comp


def calcular_media_por_dia(df):
    if COL_DATA not in df.columns:
        print(f"⚠️ Coluna '{COL_DATA}' não encontrada — não foi possível gerar médias diárias.")
        return None

    df = df.with_columns([
        pl.col(COL_DATA).str.slice(0, 10).alias(COL_DATA)
    ])

    media_dia = (
        df.group_by(COL_DATA)
        .agg([
            pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
            pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
            pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
            pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
        ])
        .sort(COL_DATA)
    )
    return media_dia


def separar_por_data(df):
    if COL_DATA not in df.columns:
        print(f"⚠️ Coluna '{COL_DATA}' não encontrada — não será separado por data.")
        return {}
    df = df.with_columns(pl.col(COL_DATA).str.slice(0, 10).alias(COL_DATA))
    datas = df.select(COL_DATA).unique().to_series().to_list()
    resultado = {}
    for data in datas:
        sub = df.filter(pl.col(COL_DATA) == data)
        sub_agrupado = (
            sub.group_by(COL_PDD_ENTREGA)
            .agg([
                pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
                pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
                pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
                pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
            ])
            .rename({COL_PDD_ENTREGA: "Base Entrega"})
        )
        resultado[data] = sub_agrupado
    return resultado


def exportar_base_consolidada(writer, df):
    """Divide automaticamente a base consolidada em várias abas se ultrapassar o limite"""
    total_linhas = df.height
    num_abas = (total_linhas // LIMITE_EXCEL) + 1

    print(f"🧾 Base consolidada contém {total_linhas:,} linhas — será dividida em {num_abas} aba(s).")

    for i in range(num_abas):
        inicio = i * LIMITE_EXCEL
        fim = min((i + 1) * LIMITE_EXCEL, total_linhas)
        aba_nome = f"Base Consolidada {i+1}"
        df.slice(inicio, fim - inicio).to_pandas().to_excel(writer, sheet_name=aba_nome, index=False)


def mostrar_resumo_executivo(comparativo_df, semana_atual_df, media_por_dia_df=None):
    """
    Exibe no terminal um resumo com:
    - maior piora / maior melhora (Tempo Total)
    - TOP 5 ofensores por Etapa 6 / 7 / 8
    - médias diárias por Data (Etapas 6/7/8), se media_por_dia_df for informado
    """
    print("\n" + "="*70)
    print("📊 --- RESUMO EXECUTIVO DA ANÁLISE ---".center(70))
    print("="*70)

    # Converter para Polars se for Pandas
    if not isinstance(comparativo_df, pl.DataFrame):
        comparativo_df = pl.from_pandas(comparativo_df)
    if not isinstance(semana_atual_df, pl.DataFrame):
        semana_atual_df = pl.from_pandas(semana_atual_df)

    # Filtrar "TOTAL GERAL" para encontrar as bases
    bases_comparativo = comparativo_df.filter(pl.col("Base Entrega") != "TOTAL GERAL")

    # Base com maior piora
    piora_df = bases_comparativo.filter(pl.col("Tempo Total Δ (h)").is_not_null()).sort("Tempo Total Δ (h)", descending=True).head(1)
    if not piora_df.is_empty():
        base_piora = piora_df['Base Entrega'][0]
        valor_piora = piora_df['Tempo Total Δ (h)'][0]
        print(f"🟥 MAIOR PIORA: A base '{base_piora}' aumentou seu tempo total em {valor_piora:.2f}h.")

    # Base com maior melhora
    melhora_df = bases_comparativo.filter(pl.col("Tempo Total Δ (h)").is_not_null()).sort("Tempo Total Δ (h)").head(1)
    if not melhora_df.is_empty():
        base_melhora = melhora_df['Base Entrega'][0]
        valor_melhora = melhora_df['Tempo Total Δ (h)'][0]
        print(f"🟩 MAIOR MELHORA: A base '{base_melhora}' reduziu seu tempo total em {abs(valor_melhora):.2f}h.")

    # ============================
    # TOP 5 OFENSORES POR ETAPA
    # ============================
    print("\n" + "-"*70)
    print("🚨 TOP 5 OFENSORES POR ETAPA (Semana Atual)".center(70))
    print("-"*70)

    semana_atual_bases = semana_atual_df.filter(pl.col("Base Entrega") != "TOTAL GERAL")

    def mostrar_top5(col_etapa: str, titulo: str):
        if col_etapa not in semana_atual_bases.columns:
            print(f"\n⚠ Coluna '{col_etapa}' não encontrada para {titulo}.")
            return
        top5 = semana_atual_bases.sort(col_etapa, descending=True).head(5)
        print(f"\n{titulo}:")
        if top5.is_empty():
            print("  - Nenhuma base encontrada para análise.")
            return
        for row in top5.iter_rows(named=True):
            print(f"  - {row['Base Entrega']}: {row[col_etapa]:.2f}h")

    mostrar_top5("Etapa 6 (h)", "🔹 TOP 5 - ETAPA 6 (SC Destino -> Base Entrega)")
    mostrar_top5("Etapa 7 (h)", "🔹 TOP 5 - ETAPA 7 (Processamento na Base)")
    mostrar_top5("Etapa 8 (h)", "🔹 TOP 5 - ETAPA 8 (Saída para Entrega)")

    # ============================
    # RESUMO DIÁRIO (POR DATA)
    # ============================
    if media_por_dia_df is not None:
        print("\n" + "-"*70)
        print("📆 MÉDIAS DIÁRIAS POR ETAPA (Semana Atual)".center(70))
        print("-"*70)

        if not isinstance(media_por_dia_df, pl.DataFrame):
            media_por_dia_df = pl.from_pandas(media_por_dia_df)

        # Garante ordenação por Data
        media_por_dia_df = media_por_dia_df.sort(COL_DATA)

        for row in media_por_dia_df.iter_rows(named=True):
            data = row[COL_DATA]
            e6 = row.get("Etapa 6 (h)", 0.0)
            e7 = row.get("Etapa 7 (h)", 0.0)
            e8 = row.get("Etapa 8 (h)", 0.0)
            tot = row.get("Tempo Total (h)", 0.0)

            print(f"\n📅 {data}")
            print(f"  - Etapa 6 (média): {e6:.2f}h")
            print(f"  - Etapa 7 (média): {e7:.2f}h")
            print(f"  - Etapa 8 (média): {e8:.2f}h")
            print(f"  - Tempo Total (média): {tot:.2f}h")

    print("="*70)


# =================== EXECUÇÃO ===================

def main():
    print("\n🚀 Iniciando análise comparativa semanal...")

    pastas = encontrar_duas_ultimas_pastas(BASE_DIR)
    if len(pastas) < 2:
        print("❌ Menos de duas pastas encontradas.")
        return

    semana_atual_path, semana_anterior_path = pastas[0], pastas[1]
    print(f"📁 Semana Atual: {os.path.basename(semana_atual_path)}")
    print(f"📁 Semana Anterior: {os.path.basename(semana_anterior_path)}")

    df_atual = ler_todos_excel(semana_atual_path)
    df_ant = ler_todos_excel(semana_anterior_path)
    if df_atual is None or df_ant is None:
        print("❌ Não foi possível ler uma das semanas.")
        return

    df_atual = filtrar_por_uf(df_atual)
    df_ant = filtrar_por_uf(df_ant)

    print("\n⏳ Calculando médias por base...")
    semana_atual, df_atual_limpo = calcular_tempo_medio(df_atual)
    semana_anterior, _ = calcular_tempo_medio(df_ant)

    print("📈 Gerando comparativo...")
    comparativo = gerar_comparativo(semana_anterior, semana_atual)

    media_por_dia = calcular_media_por_dia(df_atual_limpo)
    por_data = separar_por_data(df_atual_limpo)

    output_excel = os.path.join(OUTPUT_DIR, "Comparativo_ShippingTime_PorData.xlsx")
    with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
        # 🔹 Abas principais
        comparativo.to_pandas().to_excel(writer, sheet_name="Comparativo Semanal", index=False)
        if media_por_dia is not None:
            media_por_dia.to_pandas().to_excel(writer, sheet_name="Média por Dia (Atual)", index=False)

        # 🔹 Abas separadas por Data
        for data, df_data in por_data.items():
            safe_name = str(data).replace("/", "-")
            df_data.to_pandas().to_excel(writer, sheet_name=safe_name[:31], index=False)

        # 🔹 Abas com base consolidada (divididas)
        exportar_base_consolidada(writer, df_atual_limpo)

    print(f"\n✅ Comparativo salvo em:\n{output_excel}\n")
    print("📑 Abas criadas:")
    print("- Comparativo Semanal")
    print("- Média por Dia (Atual)")
    print("- Uma aba por Data detectada")
    print("- Base Consolidada (dividida automaticamente se necessário)")

    # ✨ RESUMO EXECUTIVO + TOP 5 + RESUMO DIÁRIO
    mostrar_resumo_executivo(comparativo, semana_atual, media_por_dia)

if __name__ == "__main__":
    main()
