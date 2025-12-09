# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import List, Tuple

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# ==========================================================
# CONFIG VISUAL
# ==========================================================
st.set_page_config(
    page_title="Painel de Entregas",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================================
# MAPEAMENTO DE COLUNAS (CHINÊS -> PT)
# ==========================================================
CHINESE_COL_MAP = {
    "实际签收15点签收-15点签收量": "Assinadas até 15h (Qtd)",
    "实际签收15点签收-15点签收率": "Assinadas até 15h (Taxa)",
}

# ==========================================================
# COLUNAS PRINCIPAIS
# ==========================================================
COL_DATA = "Data"
COL_BASE_ENTREGA = "Base de entrega"

COL_QTD_A_ENTREGAR = "Qtd a entregar"
COL_QTD_1_TENT = "Qtd com 1º tentativa de entrega"
COL_QTD_PRAZO = "Qtd entregas no prazo"
COL_QTD_ATRASO = "Qtd entregas com atraso"

DIMENSIONS_DEFAULT = [
    "Regional Remetente",
    "Base remetente",
    "Regional de entrega",
    "Nome SC Destino/HUB",
    "Coordenador",
    "Base de entrega",
    "Origem do Pedido",
    "Tipo de produto",
    "Turno de linha secundária",
    "Horário limite de entrega",
]

# ==========================================================
# UTILITÁRIOS
# ==========================================================
def _safe_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns.tolist()
    rename_map = {c: CHINESE_COL_MAP[c] for c in cols if c in CHINESE_COL_MAP}
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _ensure_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _rate(numer, denom) -> float:
    numer = 0 if pd.isna(numer) else numer
    denom = 0 if pd.isna(denom) else denom
    if denom and denom > 0:
        return float(numer) / float(denom)
    return 0.0


def _format_pct(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{x * 100:.2f}%"


def _list_excel_files(folder: str) -> List[Path]:
    p = Path(folder)
    if not p.exists() or not p.is_dir():
        return []

    files = []
    for ext in ("*.xlsx", "*.xls"):
        files.extend(p.glob(ext))

    files = [f for f in files if not f.name.startswith("~$")]
    return sorted(files)


def _files_signature(files: List[Path]) -> Tuple[Tuple[str, float], ...]:
    sig = []
    for f in files:
        try:
            sig.append((str(f), f.stat().st_mtime))
        except OSError:
            sig.append((str(f), 0.0))
    return tuple(sig)

# ==========================================================
# LEITURA (CACHE)
# ==========================================================
@st.cache_data(show_spinner=False)
def load_excel_file(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)
    df = _safe_rename_columns(df)
    df = _ensure_datetime(df, COL_DATA)

    numeric_candidates = [
        COL_QTD_A_ENTREGAR, COL_QTD_1_TENT, COL_QTD_PRAZO, COL_QTD_ATRASO,
        "Insucessos de entrega - cliente",
        "Não entregues - pct prob (outros)",
        "Não entregues - sem pct prob",
        "Assinadas até 15h (Qtd)",
    ]
    df = _coerce_numeric(df, numeric_candidates)

    df["__arquivo_origem"] = Path(path).name
    return df


@st.cache_data(show_spinner=False)
def load_from_folder_cached(folder: str, signature: Tuple[Tuple[str, float], ...]) -> pd.DataFrame:
    files = _list_excel_files(folder)
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            dfs.append(load_excel_file(str(f)))
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


@st.cache_data(show_spinner=False)
def load_from_uploads_cached(
    file_names: Tuple[str, ...],
    file_sizes: Tuple[int, ...],
    files_bytes: Tuple[bytes, ...]
) -> pd.DataFrame:
    dfs = []
    for name, b in zip(file_names, files_bytes):
        try:
            from io import BytesIO
            df = pd.read_excel(BytesIO(b), sheet_name=0)
            df = _safe_rename_columns(df)
            df = _ensure_datetime(df, COL_DATA)

            numeric_candidates = [
                COL_QTD_A_ENTREGAR, COL_QTD_1_TENT, COL_QTD_PRAZO, COL_QTD_ATRASO,
                "Insucessos de entrega - cliente",
                "Não entregues - pct prob (outros)",
                "Não entregues - sem pct prob",
                "Assinadas até 15h (Qtd)",
            ]
            df = _coerce_numeric(df, numeric_candidates)

            df["__arquivo_origem"] = name
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

# ==========================================================
# FILTROS
# ==========================================================
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.subheader("Filtros")

    if COL_DATA in df.columns:
        min_d = df[COL_DATA].min()
        max_d = df[COL_DATA].max()

        if not pd.isna(min_d) and not pd.isna(max_d):
            date_range = st.sidebar.date_input(
                "Período",
                value=(min_d.date(), max_d.date()),
                min_value=min_d.date(),
                max_value=max_d.date()
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                d1, d2 = date_range
                df = df[df[COL_DATA].between(pd.to_datetime(d1), pd.to_datetime(d2))]

    filter_cols = [
        "Regional Remetente",
        "Base remetente",
        "Regional de entrega",
        "Nome SC Destino/HUB",
        "Coordenador",
        "Base de entrega",
        "Tipo de produto",
        "Turno de linha secundária",
    ]

    for col in filter_cols:
        if col in df.columns:
            options = sorted(
                [x for x in df[col].dropna().unique().tolist() if str(x).strip() != ""]
            )
            if options:
                sel = st.sidebar.multiselect(col, options, default=[])
                if sel:
                    df = df[df[col].isin(sel)]

    return df

# ==========================================================
# KPIs
# ==========================================================
def build_kpis(df: pd.DataFrame):
    total_a_entregar = int(df[COL_QTD_A_ENTREGAR].sum()) if COL_QTD_A_ENTREGAR in df.columns else 0
    total_1_tent = int(df[COL_QTD_1_TENT].sum()) if COL_QTD_1_TENT in df.columns else 0
    total_prazo = int(df[COL_QTD_PRAZO].sum()) if COL_QTD_PRAZO in df.columns else 0
    total_atraso = int(df[COL_QTD_ATRASO].sum()) if COL_QTD_ATRASO in df.columns else 0

    nao_cols = [
        "Insucessos de entrega - cliente",
        "Não entregues - pct prob (outros)",
        "Não entregues - sem pct prob",
    ]
    total_nao_entregues = 0
    for c in nao_cols:
        if c in df.columns:
            total_nao_entregues += int(pd.to_numeric(df[c], errors="coerce").fillna(0).sum())

    assinadas_15h_qtd = int(df["Assinadas até 15h (Qtd)"].sum()) if "Assinadas até 15h (Qtd)" in df.columns else 0

    taxa_1_tent = _rate(total_1_tent, total_a_entregar)
    taxa_prazo = _rate(total_prazo, total_a_entregar)
    taxa_atraso = _rate(total_atraso, total_a_entregar)
    taxa_nao = _rate(total_nao_entregues, total_a_entregar)
    taxa_15h = _rate(assinadas_15h_qtd, total_a_entregar)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Qtd a entregar", f"{total_a_entregar:,}".replace(",", "."))
    c2.metric("1ª tentativa (Qtd)", f"{total_1_tent:,}".replace(",", "."))
    c3.metric("1ª tentativa (Taxa)", _format_pct(taxa_1_tent))
    c4.metric("SLA global (No prazo)", _format_pct(taxa_prazo))
    c5.metric("Atraso (Taxa)", _format_pct(taxa_atraso))
    c6.metric("Não entregues (Taxa)", _format_pct(taxa_nao))

    if "Assinadas até 15h (Qtd)" in df.columns:
        c7, c8, c9 = st.columns(3)
        c7.metric("Assinadas até 15h (Qtd)", f"{assinadas_15h_qtd:,}".replace(",", "."))
        c8.metric("Assinadas até 15h (Taxa calc.)", _format_pct(taxa_15h))
        c9.metric("Não entregues (Qtd)", f"{total_nao_entregues:,}".replace(",", "."))

# ==========================================================
# SLA POR BASE (O QUE VOCÊ PEDIU)
# ==========================================================
def sla_por_base(df: pd.DataFrame) -> pd.DataFrame:
    if COL_BASE_ENTREGA not in df.columns:
        return pd.DataFrame()

    if COL_QTD_A_ENTREGAR not in df.columns or COL_QTD_PRAZO not in df.columns:
        return pd.DataFrame()

    g = (
        df.groupby(COL_BASE_ENTREGA, dropna=False)[[COL_QTD_A_ENTREGAR, COL_QTD_PRAZO]]
        .sum()
        .reset_index()
    )

    g["SLA (%)"] = g.apply(
        lambda r: _rate(r[COL_QTD_PRAZO], r[COL_QTD_A_ENTREGAR]),
        axis=1
    )

    # formata base vazia
    g[COL_BASE_ENTREGA] = g[COL_BASE_ENTREGA].fillna("SEM BASE")

    return g

# ==========================================================
# RANKING GENÉRICO POR BASE
# ==========================================================
def ranking_por_base(df: pd.DataFrame) -> pd.DataFrame:
    if COL_BASE_ENTREGA not in df.columns or COL_QTD_A_ENTREGAR not in df.columns:
        return pd.DataFrame()

    agg_dict = {COL_QTD_A_ENTREGAR: "sum"}
    if COL_QTD_1_TENT in df.columns:
        agg_dict[COL_QTD_1_TENT] = "sum"
    if COL_QTD_PRAZO in df.columns:
        agg_dict[COL_QTD_PRAZO] = "sum"
    if COL_QTD_ATRASO in df.columns:
        agg_dict[COL_QTD_ATRASO] = "sum"
    if "Assinadas até 15h (Qtd)" in df.columns:
        agg_dict["Assinadas até 15h (Qtd)"] = "sum"

    nao_cols = [
        "Insucessos de entrega - cliente",
        "Não entregues - pct prob (outros)",
        "Não entregues - sem pct prob",
    ]
    for c in nao_cols:
        if c in df.columns:
            agg_dict[c] = "sum"

    g = df.groupby(COL_BASE_ENTREGA, dropna=False).agg(agg_dict).reset_index()
    g[COL_BASE_ENTREGA] = g[COL_BASE_ENTREGA].fillna("SEM BASE")

    # taxas calculadas
    g["Taxa 1ª tentativa (calc.)"] = g.apply(
        lambda r: _rate(r.get(COL_QTD_1_TENT, 0), r.get(COL_QTD_A_ENTREGAR, 0)), axis=1
    )
    g["SLA (%)"] = g.apply(
        lambda r: _rate(r.get(COL_QTD_PRAZO, 0), r.get(COL_QTD_A_ENTREGAR, 0)), axis=1
    )
    g["Taxa atraso (calc.)"] = g.apply(
        lambda r: _rate(r.get(COL_QTD_ATRASO, 0), r.get(COL_QTD_A_ENTREGAR, 0)), axis=1
    )

    def _sum_nao(row):
        s = 0
        for c in nao_cols:
            s += row.get(c, 0) if c in row else 0
        return s

    g["Qtd não entregues (calc.)"] = g.apply(_sum_nao, axis=1)
    g["Taxa não entregues (calc.)"] = g.apply(
        lambda r: _rate(r.get("Qtd não entregues (calc.)", 0), r.get(COL_QTD_A_ENTREGAR, 0)), axis=1
    )

    return g

# ==========================================================
# GRÁFICOS
# ==========================================================
def render_charts_time(df: pd.DataFrame):
    if COL_DATA not in df.columns or COL_QTD_A_ENTREGAR not in df.columns:
        st.info("Sem dados suficientes para gráfico por data.")
        return

    daily = df.groupby(COL_DATA)[[COL_QTD_A_ENTREGAR]].sum().reset_index()
    fig = px.line(daily, x=COL_DATA, y=COL_QTD_A_ENTREGAR, markers=True, title="Qtd a entregar por data")
    st.plotly_chart(fig, use_container_width=True)


def to_excel_bytes(data: pd.DataFrame) -> bytes:
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, sheet_name="filtrado")
    return output.getvalue()


# ==========================================================
# SELETOR DE PASTA (Windows local)
# ==========================================================
def try_pick_folder_windows() -> str:
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        folder = filedialog.askdirectory()
        root.destroy()
        return folder or ""
    except Exception:
        return ""

# ==========================================================
# APP
# ==========================================================
st.title("📦 Painel de Entregas")
st.caption("Inclui SLA por Base: Qtd entregas no prazo / Qtd a entregar.")

st.sidebar.subheader("Fonte dos dados")
mode = st.sidebar.radio(
    "Como você quer carregar?",
    ["Pasta local (Windows)", "Upload de arquivos"],
    index=0
)

df = pd.DataFrame()

# ---------------------------
# MODO 1: PASTA LOCAL
# ---------------------------
if mode == "Pasta local (Windows)":
    if "folder_path" not in st.session_state:
        st.session_state.folder_path = ""

    col_a, col_b = st.columns([3, 1])

    with col_a:
        folder_input = st.text_input(
            "Caminho da pasta com os Excel",
            value=st.session_state.folder_path,
            placeholder=r"C:\Users\...\MinhaPasta"
        )

    with col_b:
        if st.button("📁 Escolher pasta"):
            picked = try_pick_folder_windows()
            if picked:
                st.session_state.folder_path = picked
                folder_input = picked

    st.session_state.folder_path = folder_input.strip()
    folder = st.session_state.folder_path

    if not folder:
        st.info("Informe ou selecione uma pasta para carregar os dados.")
        st.stop()

    if not os.path.isdir(folder):
        st.error("Pasta inválida ou não encontrada.")
        st.stop()

    files = _list_excel_files(folder)
    if not files:
        st.warning("Nenhum Excel encontrado nessa pasta.")
        st.stop()

    st.success(f"{len(files)} arquivo(s) encontrado(s).")
    sig = _files_signature(files)

    with st.spinner("Lendo arquivos da pasta..."):
        df = load_from_folder_cached(folder, sig)

# ---------------------------
# MODO 2: UPLOAD
# ---------------------------
else:
    uploads = st.file_uploader(
        "Selecione um ou mais Excel (.xlsx)",
        type=["xlsx", "xls"],
        accept_multiple_files=True
    )

    if not uploads:
        st.info("Faça upload de pelo menos um arquivo.")
        st.stop()

    file_names = tuple([u.name for u in uploads])
    file_sizes = tuple([getattr(u, "size", 0) for u in uploads])
    files_bytes = tuple([u.getvalue() for u in uploads])

    with st.spinner("Lendo uploads..."):
        df = load_from_uploads_cached(file_names, file_sizes, files_bytes)

# ---------------------------
# VALIDAÇÃO FINAL
# ---------------------------
if df.empty:
    st.error("Não foi possível carregar dados válidos dos arquivos.")
    st.stop()

# ---------------------------
# FILTROS
# ---------------------------
df_f = apply_filters(df)

# ---------------------------
# TABS
# ---------------------------
tab1, tab2, tab3 = st.tabs(["Visão Geral", "SLA por Base", "Detalhe"])

with tab1:
    st.subheader("KPIs")
    build_kpis(df_f)

    st.divider()
    st.subheader("Tendência")
    render_charts_time(df_f)

with tab2:
    st.subheader("SLA por Base de entrega")

    sla_df = sla_por_base(df_f)

    if sla_df.empty:
        st.info("Preciso das colunas: 'Base de entrega', 'Qtd a entregar' e 'Qtd entregas no prazo'.")
    else:
        sort_by = st.selectbox(
            "Ordenar por",
            ["SLA (%)", "Qtd a entregar"],
            index=0
        )

        ascending = False
        sla_df = sla_df.sort_values(sort_by, ascending=ascending)

        top_n = st.slider("Top N bases", 5, 100, 20, step=5)
        view = sla_df.head(top_n).copy()

        # mostra SLA formatado
        view["SLA (%)"] = view["SLA (%)"].apply(_format_pct)

        st.dataframe(view, use_container_width=True)

        # gráfico SLA (sem formatação string)
        fig = px.bar(
            sla_df.head(top_n),
            x=COL_BASE_ENTREGA,
            y="SLA (%)",
            title="SLA por Base (Top N)"
        )
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        st.subheader("Ranking completo por base (com outras taxas)")

        rank = ranking_por_base(df_f)
        if not rank.empty:
            # exibe um recorte útil
            keep = [c for c in [
                COL_BASE_ENTREGA,
                COL_QTD_A_ENTREGAR,
                COL_QTD_PRAZO,
                "SLA (%)",
                "Taxa 1ª tentativa (calc.)",
                "Taxa atraso (calc.)",
                "Taxa não entregues (calc.)",
            ] if c in rank.columns]

            rank = rank[keep].copy()
            rank = rank.sort_values(COL_QTD_A_ENTREGAR, ascending=False)

            # formata percentuais para visualização
            for c in ["SLA (%)", "Taxa 1ª tentativa (calc.)", "Taxa atraso (calc.)", "Taxa não entregues (calc.)"]:
                if c in rank.columns:
                    rank[c] = rank[c].apply(_format_pct)

            st.dataframe(rank, use_container_width=True)

with tab3:
    st.subheader("Base detalhada (após filtros)")

    all_cols = df_f.columns.tolist()
    default_cols = [c for c in [COL_DATA] + DIMENSIONS_DEFAULT + [
        COL_QTD_A_ENTREGAR, COL_QTD_1_TENT, COL_QTD_PRAZO, COL_QTD_ATRASO,
        "Assinadas até 15h (Qtd)", "Assinadas até 15h (Taxa)",
        "Insucessos de entrega - cliente",
        "Não entregues - pct prob (outros)",
        "Não entregues - sem pct prob",
        "__arquivo_origem"
    ] if c in all_cols]

    cols_sel = st.multiselect(
        "Colunas para exibição",
        options=all_cols,
        default=default_cols
    )

    st.dataframe(df_f[cols_sel] if cols_sel else df_f, use_container_width=True)

    st.divider()
    st.subheader("Download do recorte filtrado")

    st.download_button(
        label="⬇️ Baixar Excel filtrado",
        data=to_excel_bytes(df_f),
        file_name="entregas_filtradas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with st.expander("ℹ️ Observações"):
    st.write(
        "- O SLA por base é calculado de forma ponderada pelo volume: "
        "soma(no prazo) / soma(a entregar).\n"
        "- Se você quiser, posso adicionar também SLA por Coordenador e por Regional."
    )
