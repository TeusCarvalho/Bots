# -*- coding: utf-8 -*-
"""
Relatório de Capotamento (por mês, por base, com dias seguidos + lista de dias)

✅ Agora mostra SOMENTE O DIA (1..31) nas listas e no detalhado.
"""

import os
import argparse
from datetime import timedelta

import pandas as pd


# =========================
# CONFIG (AJUSTE SE PRECISAR)
# =========================
PASTA_PADRAO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Pasta de Teste para Teste"

COL_BASE = "Nome da base"
COL_DATA = "Data"
COL_CAPOT = "Base Capotada?"


# =========================
# UTIL
# =========================
def list_excel_files(folder: str) -> list[str]:
    excel_ext = (".xlsx", ".xlsm", ".xls")
    files = []
    for root, _, filenames in os.walk(folder):
        for fn in filenames:
            if fn.lower().endswith(excel_ext) and not fn.startswith("~$"):
                files.append(os.path.join(root, fn))
    return sorted(files)


def read_excel_all_sheets(path: str) -> pd.DataFrame:
    xls = pd.read_excel(path, sheet_name=None, dtype="object")
    dfs = []
    for sheet_name, df in xls.items():
        if df is None or df.empty:
            continue
        df = df.copy()
        df["_arquivo"] = os.path.basename(path)
        df["_aba"] = sheet_name
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def parse_date(series: pd.Series) -> pd.Series:
    s = series.copy()
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_datetime(s, errors="coerce", origin="1899-12-30", unit="D")
    return pd.to_datetime(s, errors="coerce", dayfirst=True)


def capot_flag(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip().str.upper()
    true_set = {"Y", "YES", "SIM", "S", "1", "TRUE"}
    return s.isin(true_set)


def build_streak_segments(dates: pd.Series) -> list[tuple[pd.Timestamp, pd.Timestamp, int]]:
    dates = pd.to_datetime(dates, errors="coerce").dropna()
    if dates.empty:
        return []

    uniq = sorted(pd.Series(dates.dt.normalize().unique()))
    if not uniq:
        return []

    segments = []
    start = uniq[0]
    prev = uniq[0]

    for d in uniq[1:]:
        if d == prev + timedelta(days=1):
            prev = d
        else:
            length = (prev - start).days + 1
            segments.append((start, prev, length))
            start = d
            prev = d

    length = (prev - start).days + 1
    segments.append((start, prev, length))
    return segments


def summarize_streak(dates: pd.Series) -> pd.Series:
    segs = build_streak_segments(dates)
    if not segs:
        return pd.Series({"maior_seq_dias": 0, "inicio_seq": pd.NaT, "fim_seq": pd.NaT})
    best = max(segs, key=lambda x: x[2])
    return pd.Series({"maior_seq_dias": best[2], "inicio_seq": best[0], "fim_seq": best[1]})


def ensure_cols(df: pd.DataFrame, cols_defaults: dict) -> pd.DataFrame:
    for col, default in cols_defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# ✅ agora retorna só o DIA (1..31)
def days_list_str(dates: pd.Series) -> str:
    d = pd.to_datetime(dates, errors="coerce").dropna().dt.normalize()
    if d.empty:
        return ""
    uniq = sorted(pd.Series(d.unique()))
    return "; ".join(str(pd.Timestamp(x).day) for x in uniq)


# ✅ agora retorna só o DIA (1..31) da sequência
def streak_days_str(inicio, fim) -> str:
    if pd.isna(inicio) or pd.isna(fim):
        return ""
    rng = pd.date_range(start=pd.to_datetime(inicio), end=pd.to_datetime(fim), freq="D")
    return "; ".join(str(x.day) for x in rng)


# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pasta", default=PASTA_PADRAO, help="Pasta onde estão os Excels")
    parser.add_argument("--col-base", default=COL_BASE, help="Nome da coluna da base")
    parser.add_argument("--col-data", default=COL_DATA, help="Nome da coluna da data")
    parser.add_argument("--col-capot", default=COL_CAPOT, help="Nome da coluna Y/N (capotou)")
    parser.add_argument("--output", default=None, help="Caminho do Excel de saída (opcional)")
    parser.add_argument("--salvar-dados", action="store_true", help="Salvar aba Dados_Consolidados")
    args = parser.parse_args()

    pasta = args.pasta
    col_base = args.col_base
    col_data = args.col_data
    col_capot = args.col_capot

    if not os.path.isdir(pasta):
        raise FileNotFoundError(f"Pasta não encontrada: {pasta}")

    files = list_excel_files(pasta)
    if not files:
        raise FileNotFoundError(f"Nenhum Excel encontrado na pasta: {pasta}")

    all_dfs = []
    for f in files:
        df_i = read_excel_all_sheets(f)
        if not df_i.empty:
            all_dfs.append(df_i)

    if not all_dfs:
        raise ValueError("Nenhum dado válido encontrado nos arquivos.")

    df = pd.concat(all_dfs, ignore_index=True)
    df.columns = df.columns.astype(str).str.strip()

    missing = [c for c in [col_base, col_data, col_capot] if c not in df.columns]
    if missing:
        raise ValueError(
            "Colunas não encontradas: " + ", ".join(missing)
            + "\nPrévia colunas: " + ", ".join(map(str, df.columns[:40]))
        )

    df["_base"] = df[col_base].astype(str).str.strip()
    df["_data"] = parse_date(df[col_data])
    df["_capotou"] = capot_flag(df[col_capot])

    df = df[df["_data"].notna()].copy()
    df["_dia"] = df["_data"].dt.normalize()
    df["_mes"] = df["_data"].dt.to_period("M").astype(str)

    all_months = sorted(df["_mes"].unique().tolist())

    cap = df[df["_capotou"]].copy()

    # =========================
    # DIAS DETALHADO (agora só dia do mês)
    # =========================
    if cap.empty:
        dias_detalhado = pd.DataFrame(columns=["Mes", "Base", "Dia"])
    else:
        dias_detalhado = (
            cap[["_mes", "_base", "_dia"]]
            .drop_duplicates()
            .assign(Dia=lambda x: pd.to_datetime(x["_dia"]).dt.day.astype(int))
            .rename(columns={"_mes": "Mes", "_base": "Base"})
            .drop(columns=["_dia"])
            .sort_values(["Mes", "Base", "Dia"], ascending=[True, True, True])
        )

    # =========================
    # POR BASE / MÊS
    # =========================
    if cap.empty:
        por_base_mes = pd.DataFrame(columns=[
            "Mes", "Base", "capotamentos_linhas", "dias_com_capot",
            "maior_seq_dias", "inicio_seq", "fim_seq",
            "dias_capotamento", "dias_maior_sequencia"
        ])
        sequencias = pd.DataFrame(columns=["Mes", "Base", "Inicio", "Fim", "Dias_Seguidos"])
    else:
        por_base_mes = (
            cap.groupby(["_mes", "_base"], dropna=False)
            .agg(
                capotamentos_linhas=("_capotou", "size"),
                dias_com_capot=("_dia", "nunique"),
            )
            .reset_index()
            .rename(columns={"_mes": "Mes", "_base": "Base"})
        )

        streak_base = (
            cap.groupby(["_mes", "_base"], dropna=False)
            .apply(lambda g: summarize_streak(g["_dia"]))
            .reset_index()
            .rename(columns={"_mes": "Mes", "_base": "Base"})
        )

        por_base_mes = por_base_mes.merge(streak_base, on=["Mes", "Base"], how="left")
        por_base_mes = ensure_cols(por_base_mes, {
            "maior_seq_dias": 0,
            "inicio_seq": pd.NaT,
            "fim_seq": pd.NaT,
        })
        por_base_mes["maior_seq_dias"] = pd.to_numeric(
            por_base_mes["maior_seq_dias"], errors="coerce"
        ).fillna(0).astype(int)

        # ✅ lista de dias (só DIA)
        dias_lista_base = (
            cap.groupby(["_mes", "_base"], dropna=False)["_dia"]
            .apply(days_list_str)
            .reset_index()
            .rename(columns={"_mes": "Mes", "_base": "Base", "_dia": "dias_capotamento"})
        )
        por_base_mes = por_base_mes.merge(dias_lista_base, on=["Mes", "Base"], how="left")
        por_base_mes["dias_capotamento"] = por_base_mes["dias_capotamento"].fillna("")

        # ✅ dias da maior sequência (só DIA)
        por_base_mes["dias_maior_sequencia"] = por_base_mes.apply(
            lambda r: streak_days_str(r["inicio_seq"], r["fim_seq"]) if int(r["maior_seq_dias"]) > 0 else "",
            axis=1
        )

        # Sequências completas (mantém data completa aqui porque é auditoria)
        rows = []
        for (mes, base), g in cap.groupby(["_mes", "_base"], dropna=False):
            segs = build_streak_segments(g["_dia"])
            for ini, fim, dias in segs:
                rows.append({"Mes": mes, "Base": base, "Inicio": ini, "Fim": fim, "Dias_Seguidos": dias})
        sequencias = pd.DataFrame(rows)
        if not sequencias.empty:
            sequencias = sequencias.sort_values(["Mes", "Base", "Dias_Seguidos"], ascending=[True, True, False])

    # =========================
    # GERAL / MÊS
    # =========================
    geral_mes = pd.DataFrame({"Mes": all_months})

    if cap.empty:
        geral_mes["capotamentos_linhas"] = 0
        geral_mes["dias_com_capot"] = 0
        geral_mes["maior_seq_dias"] = 0
        geral_mes["inicio_seq"] = pd.NaT
        geral_mes["fim_seq"] = pd.NaT
        geral_mes["dias_capotamento"] = ""
        geral_mes["dias_maior_sequencia"] = ""
    else:
        geral_agg = (
            cap.groupby("_mes", dropna=False)
            .agg(
                capotamentos_linhas=("_capotou", "size"),
                dias_com_capot=("_dia", "nunique"),
            )
            .reset_index()
            .rename(columns={"_mes": "Mes"})
        )

        streak_geral = (
            cap.groupby("_mes", dropna=False)
            .apply(lambda g: summarize_streak(g["_dia"]))
            .reset_index()
            .rename(columns={"_mes": "Mes"})
        )

        dias_lista_geral = (
            cap.groupby("_mes", dropna=False)["_dia"]
            .apply(days_list_str)
            .reset_index()
            .rename(columns={"_mes": "Mes", "_dia": "dias_capotamento"})
        )

        geral_mes = (
            geral_mes
            .merge(geral_agg, on="Mes", how="left")
            .merge(streak_geral, on="Mes", how="left")
            .merge(dias_lista_geral, on="Mes", how="left")
        )

        geral_mes = ensure_cols(geral_mes, {
            "capotamentos_linhas": 0,
            "dias_com_capot": 0,
            "maior_seq_dias": 0,
            "inicio_seq": pd.NaT,
            "fim_seq": pd.NaT,
            "dias_capotamento": "",
        })

        geral_mes["capotamentos_linhas"] = pd.to_numeric(geral_mes["capotamentos_linhas"], errors="coerce").fillna(0).astype(int)
        geral_mes["dias_com_capot"] = pd.to_numeric(geral_mes["dias_com_capot"], errors="coerce").fillna(0).astype(int)
        geral_mes["maior_seq_dias"] = pd.to_numeric(geral_mes["maior_seq_dias"], errors="coerce").fillna(0).astype(int)
        geral_mes["dias_capotamento"] = geral_mes["dias_capotamento"].fillna("")

        geral_mes["dias_maior_sequencia"] = geral_mes.apply(
            lambda r: streak_days_str(r["inicio_seq"], r["fim_seq"]) if int(r["maior_seq_dias"]) > 0 else "",
            axis=1
        )

    # Ordenar
    geral_mes = geral_mes.sort_values(["Mes"], ascending=True)
    if not por_base_mes.empty:
        por_base_mes = por_base_mes.sort_values(["Mes", "capotamentos_linhas"], ascending=[True, False])

    out_path = args.output or os.path.join(pasta, "relatorio_capotamento.xlsx")

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        geral_mes.to_excel(writer, index=False, sheet_name="Geral_Mes")
        por_base_mes.to_excel(writer, index=False, sheet_name="Por_Base_Mes")
        sequencias.to_excel(writer, index=False, sheet_name="Sequencias")
        dias_detalhado.to_excel(writer, index=False, sheet_name="Dias_Detalhado")

        if args.salvar_dados:
            df_out = df.copy()
            df_out["capotou"] = df_out["_capotou"].astype(int)
            df_out.to_excel(writer, index=False, sheet_name="Dados_Consolidados")

    print(f"✅ Relatório gerado: {out_path}")


if __name__ == "__main__":
    main()
