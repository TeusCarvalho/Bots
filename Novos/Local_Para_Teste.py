# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import List, Tuple, Optional, Dict

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import date

# ==========================================================
# CONFIG
# ==========================================================
st.set_page_config(
    page_title="Painel — Pedidos sem Movimentação",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================================
# MAPEAMENTO OPCIONAL (CN -> PT) — se aparecerem colunas conhecidas
# (não quebra nada se não existir)
# ==========================================================
CHINESE_COL_MAP: Dict[str, str] = {
    "运单号": "Remessa",
    "订单号": "Número do pedido",
    "目的网点": "Base de entrega",
    "网点": "Base",
    "最后轨迹时间": "Horário da última operação",
    "最后操作时间": "Horário da última operação",
    "断更天数": "Dias sem movimentação",
    "异常类型": "Tipo de anomalia",
}

# ==========================================================
# CANDIDATOS DE COLUNAS (auto-detecção)
# ==========================================================
ID_CANDIDATES = [
    "Número de pedido JMS",
    "Número do pedido",
    "Número de pedido",
    "Pedido",
    "Order",
    "Remessa",
    "Waybill",
    "运单号",
    "订单号",
]

BASE_CANDIDATES = [
    "Base de entrega",
    "Base responsável",
    "Base remetente",
    "Base",
    "Nome da base",
    "站点",
    "网点",
    "目的网点",
]

COORD_CANDIDATES = [
    "Coordenador",
    "Supervisor",
]

UNIDADE_CANDIDATES = [
    "Unidade responsável",
    "Unidade",
    "Responsável",
]

TIPO_OP_CANDIDATES = [
    "Tipo da última operação",
    "Tipo de operação",
    "Última operação",
    "Operação",
]

ULTIMA_DATA_CANDIDATES = [
    "Horário da última operação",
    "Data da última operação",
    "Última atualização",
    "Data da última movimentação",
    "Horário da última movimentação",
    "Data",
    "最后轨迹时间",
    "最后操作时间",
]

DIAS_SEM_MOV_CANDIDATES = [
    "Dias sem movimentação",
    "Dias parados",
    "Dias sem Movimentação",
    "断更天数",
]

# ==========================================================
# HELPERS
# ==========================================================
def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _safe_rename_chinese(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {c: CHINESE_COL_MAP[c] for c in df.columns if c in CHINESE_COL_MAP}
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _norm(s: str) -> str:
    s = str(s).lower().strip()
    # normalização simples sem depender de libs externas
    for ch in ["_", "-", "  "]:
        s = s.replace(ch, " ")
    return s


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None

    cols = list(df.columns)
    norm_map = {_norm(c): c for c in cols}

    # 1) match direto normalizado
    for cand in candidates:
        nc = _norm(cand)
        if nc in norm_map:
            return norm_map[nc]

    # 2) contains
    for col in cols:
        ncol = _norm(col)
        for cand in candidates:
            if _norm(cand) in ncol:
                return col

    return None


def _coerce_datetime_col(df: pd.DataFrame, col: Optional[str]) -> pd.DataFrame:
    if col and col in df.columns:
        s = df[col].astype(str).str.strip()
        # tenta padrão comum
        parsed = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        mask = parsed.isna()
        if mask.any():
            parsed.loc[mask] = pd.to_datetime(s.loc[mask], errors="coerce")
        df[col] = parsed
    return df


def _coerce_numeric_col(df: pd.DataFrame, col: Optional[str]) -> pd.DataFrame:
    if col and col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _list_excel_files(folder: str) -> List[Path]:
    p = Path(folder)
    if not p.exists() or not p.is_dir():
        return []
    files = []
    for ext in ("*.xlsx", "*.xls"):
        files.extend(p.glob(ext))
    return sorted([f for f in files if not f.name.startswith("~$")])


def _files_signature(files: List[Path]) -> Tuple[Tuple[str, float], ...]:
    sig = []
    for f in files:
        try:
            sig.append((str(f), f.stat().st_mtime))
        except OSError:
            sig.append((str(f), 0.0))
    return tuple(sig)


def _safe_min_max_dates(series: pd.Series):
    if series is None or series.empty:
        return None, None

    s = pd.to_datetime(series.astype(str).str.strip(), errors="coerce").dropna()
    if s.empty:
        return None, None

    mn, mx = s.min(), s.max()
    if pd.isna(mn) or pd.isna(mx):
        return None, None

    if mn > mx:
        mn, mx = mx, mn

    return mn, mx


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
# LEITURA (CACHE)
# ==========================================================
@st.cache_data(show_spinner=False)
def load_excel_file(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)

    df = _clean_columns(df)
    df = _safe_rename_chinese(df)
    df = _clean_columns(df)

    # auto-detect cols
    last_dt_col = _find_col(df, ULTIMA_DATA_CANDIDATES)
    days_col = _find_col(df, DIAS_SEM_MOV_CANDIDATES)

    df = _coerce_datetime_col(df, last_dt_col)
    df = _coerce_numeric_col(df, days_col)

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
    from io import BytesIO

    dfs = []
    for name, b in zip(file_names, files_bytes):
        try:
            df = pd.read_excel(BytesIO(b), sheet_name=0)
            df = _clean_columns(df)
            df = _safe_rename_chinese(df)
            df = _clean_columns(df)

            last_dt_col = _find_col(df, ULTIMA_DATA_CANDIDATES)
            days_col = _find_col(df, DIAS_SEM_MOV_CANDIDATES)

            df = _coerce_datetime_col(df, last_dt_col)
            df = _coerce_numeric_col(df, days_col)

            df["__arquivo_origem"] = name
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# ==========================================================
# ENRIQUECIMENTO (calcula dias se necessário)
# ==========================================================
def enrich_sem_mov(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    id_col = _find_col(df, ID_CANDIDATES)
    base_col = _find_col(df, BASE_CANDIDATES)
    coord_col = _find_col(df, COORD_CANDIDATES)
    unidade_col = _find_col(df, UNIDADE_CANDIDATES)
    tipo_col = _find_col(df, TIPO_OP_CANDIDATES)
    last_dt_col = _find_col(df, ULTIMA_DATA_CANDIDATES)
    days_col = _find_col(df, DIAS_SEM_MOV_CANDIDATES)

    df["__id_col"] = id_col or ""
    df["__base_col"] = base_col or ""
    df["__coord_col"] = coord_col or ""
    df["__unidade_col"] = unidade_col or ""
    df["__tipo_col"] = tipo_col or ""
    df["__last_dt_col"] = last_dt_col or ""
    df["__days_col"] = days_col or ""

    # colunas operacionais padronizadas (para usar no app)
    if id_col and id_col in df.columns:
        df["Pedido/Remessa"] = df[id_col].astype(str)

    if base_col and base_col in df.columns:
        df["Base (auto)"] = df[base_col].fillna("SEM BASE").astype(str)

    if coord_col and coord_col in df.columns:
        df["Coordenador (auto)"] = df[coord_col].fillna("SEM COORD").astype(str)

    if unidade_col and unidade_col in df.columns:
        df["Unidade (auto)"] = df[unidade_col].fillna("SEM UNIDADE").astype(str)

    if tipo_col and tipo_col in df.columns:
        df["Tipo operação (auto)"] = df[tipo_col].fillna("SEM TIPO").astype(str)

    # define data de última movimentação
    if last_dt_col and last_dt_col in df.columns:
        df["Última movimentação (auto)"] = pd.to_datetime(df[last_dt_col], errors="coerce")

    # dias sem movimentação
    if days_col and days_col in df.columns:
        df["Dias sem mov (auto)"] = pd.to_numeric(df[days_col], errors="coerce")
    else:
        df["Dias sem mov (auto)"] = np.nan

    # se não tem dias explícitos, tenta calcular pelo last_dt
    if "Última movimentação (auto)" in df.columns:
        missing = df["Dias sem mov (auto)"].isna()
        if missing.any():
            today = pd.to_datetime(date.today())
            delta = (today - df.loc[missing, "Última movimentação (auto)"]).dt.days
            df.loc[missing, "Dias sem mov (auto)"] = delta

    df["Dias sem mov (auto)"] = pd.to_numeric(df["Dias sem mov (auto)"], errors="coerce").fillna(0).astype(int)

    return df


# ==========================================================
# FILTROS
# ==========================================================
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.subheader("Filtros")

    # filtro por data de última movimentação
    if "Última movimentação (auto)" in df.columns:
        min_ts, max_ts = _safe_min_max_dates(df["Última movimentação (auto)"])
        if min_ts is not None and max_ts is not None:
            date_range = st.sidebar.date_input(
                "Período (Última movimentação)",
                value=(min_ts.date(), max_ts.date()),
                min_value=min_ts.date(),
                max_value=max_ts.date()
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                d1, d2 = date_range
                col_dt = pd.to_datetime(df["Última movimentação (auto)"], errors="coerce")
                df = df[col_dt.between(pd.to_datetime(d1), pd.to_datetime(d2))]

    # filtros categóricos padronizados
    for label, col in [
        ("Base", "Base (auto)"),
        ("Coordenador", "Coordenador (auto)"),
        ("Unidade", "Unidade (auto)"),
        ("Tipo de operação", "Tipo operação (auto)"),
    ]:
        if col in df.columns:
            opts = sorted([x for x in df[col].dropna().unique().tolist() if str(x).strip() != ""])
            if opts:
                sel = st.sidebar.multiselect(label, opts, default=[])
                if sel:
                    df = df[df[col].isin(sel)]

    # faixa de dias sem mov (slider seguro)
    if "Dias sem mov (auto)" in df.columns:
        max_d = int(df["Dias sem mov (auto)"].max()) if len(df) else 0
        max_d = max(0, max_d)

        if max_d > 0:
            faixa = st.sidebar.slider(
                "Dias sem movimentação",
                min_value=0,
                max_value=max_d,
                value=(0, max_d)
            )
            df = df[df["Dias sem mov (auto)"].between(faixa[0], faixa[1])]
        else:
            st.sidebar.caption("Dias sem movimentação: sem variação para filtrar.")

    return df# ==========================================================
# KPIs
# ==========================================================
def render_kpis(df: pd.DataFrame):
    total_linhas = len(df)

    # tenta usar Pedido/Remessa como contagem única se existir
    if "Pedido/Remessa" in df.columns:
        total_pedidos = df["Pedido/Remessa"].nunique()
    else:
        total_pedidos = total_linhas

    dias = df["Dias sem mov (auto)"] if "Dias sem mov (auto)" in df.columns else pd.Series([0])

    gt_10 = int((dias > 10).sum())
    gt_20 = int((dias > 20).sum())
    gt_30 = int((dias > 30).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pedidos/Remessas", f"{total_pedidos:,}".replace(",", "."))
    c2.metric("> 10 dias sem mov", f"{gt_10:,}".replace(",", "."))
    c3.metric("> 20 dias sem mov", f"{gt_20:,}".replace(",", "."))
    c4.metric("> 30 dias sem mov", f"{gt_30:,}".replace(",", "."))


# ==========================================================
# RANKING
# ==========================================================
def ranking_simple(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if group_col not in df.columns:
        return pd.DataFrame()

    g = (
        df.groupby(group_col)
        .agg(
            qtd=("Pedido/Remessa", "nunique") if "Pedido/Remessa" in df.columns else ("Dias sem mov (auto)", "size"),
            dias_medio=("Dias sem mov (auto)", "mean"),
            dias_max=("Dias sem mov (auto)", "max"),
        )
        .reset_index()
        .rename(columns={group_col: "Grupo"})
        .sort_values("qtd", ascending=False)
    )

    g["dias_medio"] = g["dias_medio"].round(2)
    return g


# ==========================================================
# TENDÊNCIA COM DIAS SEM OCORRÊNCIA
# ==========================================================
def tendencia_fill(df: pd.DataFrame) -> pd.DataFrame:
    if "Última movimentação (auto)" not in df.columns:
        return pd.DataFrame()

    temp = df.dropna(subset=["Última movimentação (auto)"]).copy()
    if temp.empty:
        return pd.DataFrame()

    temp["dia"] = pd.to_datetime(temp["Última movimentação (auto)"], errors="coerce").dt.normalize()
    temp = temp.dropna(subset=["dia"])

    if temp.empty:
        return pd.DataFrame()

    # contagem por dia (preferindo unique pedidos)
    if "Pedido/Remessa" in temp.columns:
        g = temp.groupby("dia")["Pedido/Remessa"].nunique().reset_index(name="qtd")
    else:
        g = temp.groupby("dia").size().reset_index(name="qtd")

    g = g.sort_values("dia")

    min_d = g["dia"].min()
    max_d = g["dia"].max()
    full = pd.DataFrame({"dia": pd.date_range(min_d, max_d, freq="D")})

    g = full.merge(g, on="dia", how="left")
    g["qtd"] = g["qtd"].fillna(0).astype(int)

    return g


def render_trend_bar(df: pd.DataFrame):
    trend = tendencia_fill(df)
    if trend.empty:
        st.info("Sem dados de última movimentação para tendência.")
        return

    fig = px.bar(
        trend,
        x="dia",
        y="qtd",
        text="qtd",
        title="Tendência — Última movimentação (com dias sem ocorrência)"
    )
    fig.update_traces(textposition="outside", cliponaxis=False)
    st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# TOP 10 DISTRIBUIÇÃO (SEM PIZZA)
# ==========================================================
def render_distribution_top10(df: pd.DataFrame):
    dims = [c for c in ["Base (auto)", "Coordenador (auto)", "Unidade (auto)", "Tipo operação (auto)"] if c in df.columns]
    if not dims:
        st.caption("Sem dimensões padronizadas para distribuição.")
        return

    dim = st.selectbox("Dimensão", dims, index=0)

    if "Pedido/Remessa" in df.columns:
        dist = (
            df.groupby(dim)["Pedido/Remessa"].nunique()
            .reset_index(name="qtd")
            .sort_values("qtd", ascending=False)
            .head(10)
        )
    else:
        dist = (
            df.groupby(dim).size()
            .reset_index(name="qtd")
            .sort_values("qtd", ascending=False)
            .head(10)
        )

    fig = px.bar(dist, x=dim, y="qtd", text="qtd", title=f"Top 10 — {dim}")
    fig.update_traces(textposition="outside", cliponaxis=False)
    st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# DOWNLOAD
# ==========================================================
def to_excel_bytes(data: pd.DataFrame) -> bytes:
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, sheet_name="filtrado")
    return output.getvalue()


# ==========================================================
# APP
# ==========================================================
st.title("🚚 Painel — Pedidos sem Movimentação")
st.caption("Leitura por pasta ou upload • auto-detecção de colunas • KPIs, ranking e tendência.")

# ---------------------------
# Fonte de dados
# ---------------------------
st.sidebar.subheader("Fonte dos dados")
mode = st.sidebar.radio(
    "Como você quer carregar?",
    ["Pasta local (Windows)", "Upload de arquivos"],
    index=0
)

df = pd.DataFrame()

# ---------------------------
# MODO PASTA
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
# MODO UPLOAD
# ---------------------------
else:
    uploads = st.file_uploader(
        "Selecione um ou mais Excel (.xlsx/.xls)",
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
# Validação
# ---------------------------
if df.empty:
    st.error("Não foi possível carregar dados válidos dos arquivos.")
    st.stop()

# ---------------------------
# Enriquecimento + filtros
# ---------------------------
df = enrich_sem_mov(df)

# ---------------------------
# CONTROLES DE RANKING NO SIDEBAR
# ---------------------------
st.sidebar.divider()
st.sidebar.subheader("Rankings")

ranking_dims = [c for c in ["Base (auto)", "Coordenador (auto)", "Unidade (auto)", "Tipo operação (auto)"] if c in df.columns]
if not ranking_dims:
    ranking_dims = ["__arquivo_origem"]

ranking_group = st.sidebar.selectbox("Agrupar por", ranking_dims, index=0)
ranking_top_n = st.sidebar.slider("Top N", 5, 50, 15, step=5)

# aplica filtros depois de definir dimensões
df_f = apply_filters(df)

# ---------------------------
# TABS
# ---------------------------
tab1, tab2, tab3 = st.tabs(["Visão Geral", "Rankings", "Detalhe"])

with tab1:
    st.subheader("KPIs")
    render_kpis(df_f)

    st.divider()
    st.subheader("Tendência (por última movimentação)")
    render_trend_bar(df_f)

    st.divider()
    st.subheader("Distribuição operacional (Top 10)")
    render_distribution_top10(df_f)

with tab2:
    st.subheader("Ranking (com filtros laterais)")
    rank = ranking_simple(df_f, ranking_group)

    if rank.empty:
        st.info("Sem dados suficientes para ranking nessa dimensão.")
    else:
        view = rank.head(ranking_top_n).copy()
        st.dataframe(view, use_container_width=True)

        fig = px.bar(view, x="Grupo", y="qtd", text="qtd",
                     title=f"Top {ranking_top_n} — {ranking_group}")
        fig.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Dados filtrados")

    all_cols = df_f.columns.tolist()
    default_cols = [c for c in [
        "Pedido/Remessa",
        "Base (auto)",
        "Coordenador (auto)",
        "Unidade (auto)",
        "Tipo operação (auto)",
        "Última movimentação (auto)",
        "Dias sem mov (auto)",
        "__arquivo_origem",
    ] if c in all_cols]

    cols_sel = st.multiselect("Colunas para exibição", options=all_cols, default=default_cols)
    st.dataframe(df_f[cols_sel] if cols_sel else df_f, use_container_width=True)

    st.divider()
    st.subheader("Download")

    st.download_button(
        label="⬇️ Baixar Excel filtrado",
        data=to_excel_bytes(df_f),
        file_name="pedidos_sem_mov_filtrado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with st.expander("ℹ️ Notas rápidas"):
    st.write(
        "- O painel tenta identificar automaticamente colunas do relatório.\n"
        "- Se existir uma coluna de dias sem movimentação, ele usa.\n"
        "- Se não existir, calcula com base na última movimentação.\n"
        "- Tendência em barras preenche dias sem ocorrência com 0 e mostra os números."
    )

