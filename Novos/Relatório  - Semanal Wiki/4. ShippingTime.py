# -*- coding: utf-8 -*-

import polars as pl
import pandas as pd
import os
import glob
import re
from datetime import datetime
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")

# =================== CONFIG ===================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\1. Shipping Time"
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

UFS_PERMITIDAS = ["PA", "MT", "GO", "AM", "MS", "RO", "TO", "DF", "RR", "AC", "AP"]
LIMITE_EXCEL = 1_048_000

# (Opcional) Override manual, se quiser travar quais semanas comparar:
SEMANA_ATUAL_OVERRIDE = None   # ex: r"...\Semana_50_2025"
SEMANA_ANT_OVERRIDE   = None   # ex: r"...\Semana_49_2025"

# COLUNAS
COL_PDD_ENTREGA = "PDD de Entrega"
COL_ESTADO_ENTREGA = "Estado de Entrega"
COL_DATA = "Data"
COL_ETAPA_6 = "Tempo trânsito SC Destino->Base Entrega"
COL_ETAPA_7 = "Tempo médio processamento Base Entrega"
COL_ETAPA_8 = "Tempo médio Saída para Entrega->Entrega"
COL_TEMPO_TOTAL = "Tempo Total (h)"


def _arquivos_excel_na_pasta(pasta: str):
    arquivos = [
        arq for arq in glob.glob(os.path.join(pasta, "*.xls*"))
        if not os.path.basename(arq).startswith("~$")
    ]
    return arquivos


def _extrair_data_do_nome(nome_pasta: str):
    """
    Tenta extrair uma "chave temporal" do nome da pasta.
    Retorna datetime (sem hora) ou None.
    Suporta:
      - YYYY-MM-DD / YYYY_MM_DD / YYYY.MM.DD
      - DD-MM-YYYY / DD_MM_YYYY / DD.MM.YYYY
      - 2025W50 / 2025-W50 / 2025 W50
      - Semana 50 2025 / 2025 Semana 50
    """
    s = nome_pasta.strip()

    datas = []

    # YYYY-MM-DD
    for y, m, d in re.findall(r"(\d{4})[._/-](\d{2})[._/-](\d{2})", s):
        try:
            datas.append(datetime(int(y), int(m), int(d)))
        except Exception:
            pass

    # DD-MM-YYYY
    for d, m, y in re.findall(r"(\d{2})[._/-](\d{2})[._/-](\d{4})", s):
        try:
            datas.append(datetime(int(y), int(m), int(d)))
        except Exception:
            pass

    if datas:
        # Se tiver intervalo (duas datas), usa a maior (normalmente fim da semana)
        return max(datas)

    # 2025W50 / 2025-W50
    m = re.search(r"(\d{4})\s*[-_ ]?\s*W\s*([0-5]?\d)", s, flags=re.IGNORECASE)
    if m:
        y = int(m.group(1))
        w = int(m.group(2))
        try:
            # usa domingo da ISO-week como "fim da semana"
            return datetime.fromisocalendar(y, w, 7)
        except Exception:
            return None

    # Semana 50 2025 / 2025 Semana 50
    m = re.search(r"(?:semana)\s*([0-5]?\d)\s*(\d{4})", s, flags=re.IGNORECASE)
    if m:
        w = int(m.group(1))
        y = int(m.group(2))
        try:
            return datetime.fromisocalendar(y, w, 7)
        except Exception:
            return None

    m = re.search(r"(\d{4})\s*(?:semana)\s*([0-5]?\d)", s, flags=re.IGNORECASE)
    if m:
        y = int(m.group(1))
        w = int(m.group(2))
        try:
            return datetime.fromisocalendar(y, w, 7)
        except Exception:
            return None

    return None


def encontrar_duas_ultimas_pastas(path: str):
    """
    Versão robusta:
    1) ignora "Output"
    2) só considera pastas com Excel
    3) ordena por data no nome; fallback: mtime do Excel mais recente
    """
    candidatos = []

    for p in os.listdir(path):
        full = os.path.join(path, p)
        if not os.path.isdir(full):
            continue
        if "output" in p.lower():
            continue

        arquivos = _arquivos_excel_na_pasta(full)
        if not arquivos:
            continue

        chave_nome = _extrair_data_do_nome(p)
        if chave_nome is not None:
            chave = chave_nome
            fonte = "nome_da_pasta"
        else:
            ultimo_mtime = max(os.path.getmtime(a) for a in arquivos)
            chave = datetime.fromtimestamp(ultimo_mtime)
            fonte = "mtime_do_excel"

        candidatos.append((chave, full, p, fonte))

    candidatos.sort(key=lambda x: x[0], reverse=True)

    print("\n📅 Pastas candidatas (ordenadas pela chave temporal):")
    if not candidatos:
        print("  (nenhuma pasta válida com Excel encontrada)")
        return []

    for chave, _, nome, fonte in candidatos[:20]:
        print(f"  - {nome} | chave={chave.strftime('%Y-%m-%d %H:%M:%S')} | fonte={fonte}")

    return [c[1] for c in candidatos[:2]]
def ler_todos_excel(pasta):
    arquivos = _arquivos_excel_na_pasta(pasta)

    if not arquivos:
        print(f"⚠️ Nenhum arquivo Excel encontrado em: {pasta}")
        return None

    print(f"\n📂 Lendo planilhas da pasta: {os.path.basename(pasta)}")
    dfs = []

    pbar = tqdm(arquivos, desc="📊 Processando arquivos", unit="arquivo")
    for arq in pbar:
        nome = os.path.basename(arq)
        pbar.set_postfix_str(nome[:60])
        try:
            df = pl.read_excel(arq)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Erro ao ler {nome}: {e}")

    if not dfs:
        print("❌ Nenhum arquivo foi lido com sucesso.")
        return None

    final = pl.concat(dfs, how="vertical")
    final = final.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in final.columns])
    return final


def filtrar_por_uf(df):
    if COL_ESTADO_ENTREGA not in df.columns:
        print(f"⚠️ Coluna '{COL_ESTADO_ENTREGA}' não encontrada.")
        return df

    df = df.filter(pl.col(COL_ESTADO_ENTREGA).is_in(UFS_PERMITIDAS))
    print("✅ UFs filtradas:", ", ".join(UFS_PERMITIDAS))
    return df


def limpar_coluna_num(df, col):
    return (
        df[col]
        .str.replace_all(r"[^\d,.\-]", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
        .fill_nan(0)
        .fill_null(0)
    )


def calcular_tempo_medio(df):
    for col in [COL_ETAPA_6, COL_ETAPA_7, COL_ETAPA_8]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    df = df.with_columns([
        limpar_coluna_num(df, COL_ETAPA_6).alias(COL_ETAPA_6),
        limpar_coluna_num(df, COL_ETAPA_7).alias(COL_ETAPA_7),
        limpar_coluna_num(df, COL_ETAPA_8).alias(COL_ETAPA_8),
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

    total = agrupado.select([
        pl.lit("TOTAL GERAL").alias("Base Entrega"),
        pl.col("Etapa 6 (h)").mean(),
        pl.col("Etapa 7 (h)").mean(),
        pl.col("Etapa 8 (h)").mean(),
        pl.col("Tempo Total (h)").mean()
    ])

    return pl.concat([agrupado, total], how="vertical"), df


def gerar_comparativo(ant, atual):
    comp = ant.join(atual, on="Base Entrega", how="outer", suffix="_Atual")

    for etapa in ["Etapa 6", "Etapa 7", "Etapa 8", "Tempo Total"]:
        comp = comp.with_columns([
            (pl.col(f"{etapa} (h)_Atual") - pl.col(f"{etapa} (h)"))
            .cast(pl.Float64, strict=False)
            .alias(f"{etapa} Δ (h)")
        ])

    return comp


def calcular_media_por_dia(df):
    if COL_DATA not in df.columns:
        return None

    df = df.with_columns(pl.col(COL_DATA).str.slice(0, 10).alias(COL_DATA))

    return (
        df.group_by(COL_DATA)
        .agg([
            pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
            pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
            pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
            pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
        ])
        .sort(COL_DATA)
    )


def separar_por_data(df):
    if COL_DATA not in df.columns:
        return {}

    df = df.with_columns(pl.col(COL_DATA).str.slice(0, 10).alias(COL_DATA))
    datas = df.select(COL_DATA).unique().to_series().to_list()

    out = {}
    for d in datas:
        sub = df.filter(pl.col(COL_DATA) == d)
        out[d] = (
            sub.group_by(COL_PDD_ENTREGA)
            .agg([
                pl.mean(COL_ETAPA_6).alias("Etapa 6 (h)"),
                pl.mean(COL_ETAPA_7).alias("Etapa 7 (h)"),
                pl.mean(COL_ETAPA_8).alias("Etapa 8 (h)"),
                pl.mean(COL_TEMPO_TOTAL).alias("Tempo Total (h)")
            ])
            .rename({COL_PDD_ENTREGA: "Base Entrega"})
        )

    return out


def exportar_base_consolidada(writer, df):
    total = df.height
    abas = (total // LIMITE_EXCEL) + 1

    print(f"🧾 Base consolidada: {total:,} linhas → {abas} abas")

    for i in range(abas):
        ini = i * LIMITE_EXCEL
        fim = min((i + 1) * LIMITE_EXCEL, total)
        aba = f"Base Consolidada {i+1}"
        df.slice(ini, fim - ini).to_pandas().to_excel(writer, aba, index=False)


# =================== NOVO: TOP N PIORAS (Δ>0) ===================
def top_n_pioras_por_etapa(comp_df: pl.DataFrame, etapa: str, n: int = 10) -> pl.DataFrame:
    """
    Retorna TOP N piores (Δ>0) por etapa, usando a tabela 'comp_df' (comparativo).
    Ordena por Δ desc (maior piora primeiro).
    """
    base_col = "Base Entrega"
    col_ant = f"{etapa} (h)"
    col_atual = f"{etapa} (h)_Atual"
    col_delta = f"{etapa} Δ (h)"

    for c in [base_col, col_ant, col_atual, col_delta]:
        if c not in comp_df.columns:
            return pl.DataFrame()

    df = (
        comp_df
        .filter(pl.col(base_col) != "TOTAL GERAL")
        .with_columns([
            pl.col(col_ant).cast(pl.Float64, strict=False),
            pl.col(col_atual).cast(pl.Float64, strict=False),
            pl.col(col_delta).cast(pl.Float64, strict=False),
        ])
        .filter(pl.col(col_delta) > 0)  # piora = aumento
        .sort(col_delta, descending=True)
        .head(n)
        .with_row_index("Rank", offset=1)
        .select(["Rank", base_col, col_ant, col_atual, col_delta])
        .rename({
            col_ant: f"{etapa} Semana Ant (h)",
            col_atual: f"{etapa} Semana Atual (h)",
            col_delta: f"{etapa} Δ (h)"
        })
    )
    return df


def mostrar_resumo_executivo(comp_df, sem_at_df, media_dia_df=None):
    print("\n" + "=" * 70)
    print("📊 --- RESUMO EXECUTIVO ---".center(70))
    print("=" * 70)

    if not isinstance(comp_df, pl.DataFrame):
        comp_df = pl.from_pandas(comp_df)

    if not isinstance(sem_at_df, pl.DataFrame):
        sem_at_df = pl.from_pandas(sem_at_df)

    bases = comp_df.filter(pl.col("Base Entrega") != "TOTAL GERAL")

    piora = bases.sort("Tempo Total Δ (h)", descending=True).head(1)
    if not piora.is_empty():
        delta = piora["Tempo Total Δ (h)"][0]
        delta = 0 if delta is None else float(delta)
        print(f"🟥 MAIOR PIORA: {piora['Base Entrega'][0]} (+{delta:.2f}h)")

    melhora = bases.sort("Tempo Total Δ (h)").head(1)
    if not melhora.is_empty():
        delta = melhora["Tempo Total Δ (h)"][0]
        delta = 0 if delta is None else float(delta)
        print(f"🟩 MAIOR MELHORA: {melhora['Base Entrega'][0]} ({delta:.2f}h)")

    print("\n" + "-" * 70)
    print("📦 COMPARATIVO GERAL — Semana Atual vs Anterior".center(70))
    print("-" * 70)

    ant = comp_df.filter(pl.col("Base Entrega") == "TOTAL GERAL")
    at = sem_at_df.filter(pl.col("Base Entrega") == "TOTAL GERAL")

    if not ant.is_empty() and not at.is_empty():
        def seta(v): return "↑" if v > 0 else "↓"

        e6 = at["Etapa 6 (h)"][0]
        e7 = at["Etapa 7 (h)"][0]
        e8 = at["Etapa 8 (h)"][0]
        tot = at["Tempo Total (h)"][0]

        d6 = e6 - ant["Etapa 6 (h)"][0]
        d7 = e7 - ant["Etapa 7 (h)"][0]
        d8 = e8 - ant["Etapa 8 (h)"][0]
        dt = tot - ant["Tempo Total (h)"][0]

        print(f"Shipping Time: {tot:.2f}h ({seta(dt)}{abs(dt):.2f}h)")
        print(f"Etapa 6:       {e6:.2f}h ({seta(d6)}{abs(d6):.2f}h)")
        print(f"Etapa 7:       {e7:.2f}h ({seta(d7)}{abs(d7):.2f}h)")
        print(f"Etapa 8:       {e8:.2f}h ({seta(d8)}{abs(d8):.2f}h)")

    # =================== NOVO: TOP 10 PIORAS (Δ>0) ETAPA 7 e 8 ===================
    print("\n" + "-" * 70)
    print("🚨 TOP 10 PIOR VARIAÇÃO (Δ>0) — Etapas 7 e 8".center(70))
    print("-" * 70)

    def print_top_pioras(etapa: str, n: int = 10):
        df_top = top_n_pioras_por_etapa(comp_df, etapa, n=n)
        if df_top.is_empty():
            print(f"\n{etapa}: (sem dados / colunas ausentes / nenhuma piora Δ>0)")
            return

        print(f"\n🔸 {etapa} — TOP {min(n, df_top.height)} (maior aumento vs semana anterior):")
        for row in df_top.iter_rows(named=True):
            base = row["Base Entrega"]
            ant_v = row.get(f"{etapa} Semana Ant (h)")
            at_v = row.get(f"{etapa} Semana Atual (h)")
            dv = row.get(f"{etapa} Δ (h)")

            def fmt(x):
                if x is None or (isinstance(x, float) and x != x):
                    return "N/A"
                return f"{float(x):.2f}h"

            print(f"  {int(row['Rank']):02d}. {base}: {fmt(at_v)} (antes {fmt(ant_v)} | ↑{fmt(dv)})")

    print_top_pioras("Etapa 7", n=10)
    print_top_pioras("Etapa 8", n=10)

    if media_dia_df is not None:
        print("\n" + "-" * 70)
        print("📆 MÉDIAS DIÁRIAS — Semana Atual".center(70))
        print("-" * 70)

        for row in media_dia_df.iter_rows(named=True):
            print(f"\n📅 {row['Data']}")
            print(f"  - Etapa 6: {row['Etapa 6 (h)']:.2f}h")
            print(f"  - Etapa 7: {row['Etapa 7 (h)']:.2f}h")
            print(f"  - Etapa 8: {row['Etapa 8 (h)']:.2f}h")
            print(f"  - Tempo Total: {row['Tempo Total (h)']:.2f}h")

    print("=" * 70)
def main():
    print("\n🚀 Iniciando análise...")

    # Override manual (se preenchido)
    if SEMANA_ATUAL_OVERRIDE and SEMANA_ANT_OVERRIDE:
        sem_atual_pasta = SEMANA_ATUAL_OVERRIDE
        sem_ant_pasta = SEMANA_ANT_OVERRIDE
        print("\n🧷 Override manual ligado:")
        print(f"  - Semana ATUAL:    {os.path.basename(sem_atual_pasta)}")
        print(f"  - Semana ANTERIOR: {os.path.basename(sem_ant_pasta)}")
    else:
        pastas = encontrar_duas_ultimas_pastas(BASE_DIR)
        if len(pastas) < 2:
            print("❌ Não há duas semanas válidas (pastas com Excel) para comparar.")
            return

        sem_atual_pasta, sem_ant_pasta = pastas[0], pastas[1]

        print("\n✅ Semanas selecionadas para comparação:")
        print(f"  - Semana ATUAL:    {os.path.basename(sem_atual_pasta)}")
        print(f"  - Semana ANTERIOR: {os.path.basename(sem_ant_pasta)}")

    df_atual = ler_todos_excel(sem_atual_pasta)
    df_ant = ler_todos_excel(sem_ant_pasta)

    if df_atual is None or df_ant is None:
        print("❌ Falha ao ler semanas.")
        return

    df_atual = filtrar_por_uf(df_atual)
    df_ant = filtrar_por_uf(df_ant)

    sem_at, df_atual_limpo = calcular_tempo_medio(df_atual)
    sem_ant, _ = calcular_tempo_medio(df_ant)

    comp = gerar_comparativo(sem_ant, sem_at)

    media_dia = calcular_media_por_dia(df_atual_limpo)
    por_data = separar_por_data(df_atual_limpo)

    # =================== NOVO: TOP 10 PIORAS (Δ>0) para exportar no Excel ===================
    top10_e7 = top_n_pioras_por_etapa(comp, "Etapa 7", n=10)
    top10_e8 = top_n_pioras_por_etapa(comp, "Etapa 8", n=10)

    output = os.path.join(OUTPUT_DIR, "Comparativo_ShippingTime_PorData.xlsx")
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        comp.to_pandas().to_excel(writer, "Comparativo Semanal", index=False)

        # Abas novas (Top 10 pioras)
        if not top10_e7.is_empty():
            top10_e7.to_pandas().to_excel(writer, "Top10 Piora Etapa 7", index=False)
        if not top10_e8.is_empty():
            top10_e8.to_pandas().to_excel(writer, "Top10 Piora Etapa 8", index=False)

        if media_dia is not None:
            media_dia.to_pandas().to_excel(writer, "Média por Dia", index=False)

        for d, df_dia in por_data.items():
            aba = str(d).replace("/", "-")[:31]
            df_dia.to_pandas().to_excel(writer, aba, index=False)

        exportar_base_consolidada(writer, df_atual_limpo)

    print(f"\n📁 Arquivo salvo em:\n{output}\n")

    mostrar_resumo_executivo(comp, sem_at, media_dia)


if __name__ == "__main__":
    main()
