# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import List, Tuple, Optional

import streamlit as st
import pandas as pd
import plotly.express as px

# ==========================================================
# CONFIG
# ==========================================================
st.set_page_config(
    page_title="Painel de Arbitragem",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================================
# COLUNAS ESPERADAS (se existirem)
# ==========================================================
COL_NUMERO = "Número de declaração"
COL_REMESSA = "Remessa"
COL_PRODUTO = "Tipo de produto"

COL_ANOM_PRIM = "Tipo de anomalia primária"
COL_ANOM_SEC = "Tipo de anomalia secundária"

COL_STATUS_ARB = "Status de arbitragem"
COL_TIPO_DECISAO = "Tipo de decisão"

COL_BASE_REM = "Base remetente"
COL_BASE_RESP = "Base responsável"
COL_BASE_FIN = "Base de liquidação financeira"

COL_REG_REM = "Regional Remetente"
COL_REG_DECL = "Regional de declaração"
COL_REG_RESP = "Regional responsável"

COL_DECLARANTE = "Declarante"

# ✅ Coordenador
COL_COORDENADOR = "Coordenador"

# ✅ VALORES
COL_VALOR_NOVO = "Valor a pagar (yuan)"          # coluna atual
COL_VALOR_RS = "Valor a pagar (R$)"             # coluna de exibição (sem conversão)
COL_VALOR_ANTIGO = "Valor da arbitragem (yuan)" # fallback

COL_PESO = "Peso cobrável"

# ✅ DATAS PRINCIPAIS
COL_DATA_FECHAMENTO = "Data de fechamento"
COL_DATA_DECLARACAO = "Data de declaração"

# Datas comuns nesse tipo de export
DATE_COL_CANDIDATES = [
    COL_DATA_FECHAMENTO,
    COL_DATA_DECLARACAO,
    "Data de recebimento da arbitragem",
    "Data de distribuição da arbitragem",
    "Data de decisão de arbitragem",
    "Data de contestação",
    "Data de distribuição da contestação",
    "Data de decisão da contestação",
    "Tempo de processamento de retorno",
    "Hora de envio",
    "Horário de coleta",
    "Horário de Previsão de Entrega SLA Cadeia",
    "Horário da entrega",
]


# ==========================================================
# HELPERS
# ==========================================================
def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
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


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _coerce_datetime(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Robustíssimo para datas como:
    2025-11-29 15:07:57
    """
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str).str.strip()

            parsed = pd.to_datetime(
                s, format="%Y-%m-%d %H:%M:%S", errors="coerce"
            )
            mask = parsed.isna()
            if mask.any():
                parsed.loc[mask] = pd.to_datetime(s.loc[mask], errors="coerce")

            df[c] = parsed
    return df


def _resolve_valor_column(df: pd.DataFrame) -> Optional[str]:
    if COL_VALOR_NOVO in df.columns:
        return COL_VALOR_NOVO
    if COL_VALOR_ANTIGO in df.columns:
        return COL_VALOR_ANTIGO
    return None


def _ensure_valor_internal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria:
      - __valor_base -> usado nos cálculos
      - Valor a pagar (R$) -> exibição SEM conversão
    """
    col_origem = _resolve_valor_column(df)

    if col_origem:
        df[col_origem] = pd.to_numeric(df[col_origem], errors="coerce").fillna(0)
        df["__valor_base"] = df[col_origem]
        df[COL_VALOR_RS] = df[col_origem]
    else:
        df["__valor_base"] = 0
        df[COL_VALOR_RS] = 0

    return df


def _get_primary_date_col(df: pd.DataFrame) -> Optional[str]:
    if COL_DATA_FECHAMENTO in df.columns:
        return COL_DATA_FECHAMENTO
    if COL_DATA_DECLARACAO in df.columns:
        return COL_DATA_DECLARACAO
    return None


def _safe_min_max_dates(series: pd.Series):
    if series is None or series.empty:
        return None, None

    s = pd.to_datetime(series.astype(str).str.strip(), errors="coerce").dropna()
    if s.empty:
        return None, None

    min_ts, max_ts = s.min(), s.max()
    if pd.isna(min_ts) or pd.isna(max_ts):
        return None, None

    if min_ts > max_ts:
        min_ts, max_ts = max_ts, min_ts

    return min_ts, max_ts


def _format_brl_number(value: float) -> str:
    try:
        return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0,00"


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

    df = _coerce_datetime(df, DATE_COL_CANDIDATES)

    # ✅ sem atraso aqui também
    df = _coerce_numeric(df, [
        COL_PESO,
        COL_VALOR_NOVO,
        COL_VALOR_ANTIGO,
    ])

    df = _ensure_valor_internal(df)

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

            df = _coerce_datetime(df, DATE_COL_CANDIDATES)

            df = _coerce_numeric(df, [
                COL_PESO,
                COL_VALOR_NOVO,
                COL_VALOR_ANTIGO,
            ])

            df = _ensure_valor_internal(df)

            df["__arquivo_origem"] = name
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# ==========================================================
# FILTROS (SEM "Dias de atraso")
# ==========================================================
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.subheader("Filtros")

    # Data principal
    date_col = _get_primary_date_col(df)
    if date_col:
        min_ts, max_ts = _safe_min_max_dates(df[date_col])
        if min_ts is not None and max_ts is not None:
            date_range = st.sidebar.date_input(
                f"Período ({date_col})",
                value=(min_ts.date(), max_ts.date()),
                min_value=min_ts.date(),
                max_value=max_ts.date()
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                d1, d2 = date_range
                col_dt = pd.to_datetime(df[date_col].astype(str).str.strip(), errors="coerce")
                df = df[col_dt.between(pd.to_datetime(d1), pd.to_datetime(d2))]

    # Categóricos
    cat_cols = [
        COL_COORDENADOR,
        COL_REG_REM, COL_REG_DECL, COL_REG_RESP,
        COL_BASE_REM, COL_BASE_RESP, COL_BASE_FIN,
        COL_PRODUTO,
        COL_ANOM_PRIM, COL_ANOM_SEC,
        COL_DECLARANTE,
        "Origem da Solicitação",
        "Origem do Pedido",
        "Fonte",
        COL_STATUS_ARB,
        COL_TIPO_DECISAO,
    ]

    for col in cat_cols:
        if col in df.columns:
            opts = sorted([x for x in df[col].dropna().unique().tolist() if str(x).strip() != ""])
            if opts:
                sel = st.sidebar.multiselect(col, opts, default=[])
                if sel:
                    df = df[df[col].isin(sel)]

    return df


# ==========================================================
# KPIs (SEM "Atraso médio")
# ==========================================================
def render_kpis(df: pd.DataFrame):
    total = len(df)

    valor_total = df["__valor_base"].sum() if "__valor_base" in df.columns else 0
    valor_medio = df["__valor_base"].mean() if "__valor_base" in df.columns else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Declarações", f"{total:,}".replace(",", "."))
    c2.metric("Valor total (R$)", f"R$ {_format_brl_number(valor_total)}")
    c3.metric("Valor médio (R$)", f"R$ {_format_brl_number(valor_medio)}")


# ==========================================================
# RANKINGS (SEM "atraso_medio")
# ==========================================================
def ranking_simple(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if group_col not in df.columns:
        return pd.DataFrame()

    agg = {"__count": (group_col, "size")}

    if "__valor_base" in df.columns:
        agg["valor_total"] = ("__valor_base", "sum")
        agg["valor_medio"] = ("__valor_base", "mean")

    g = df.groupby(group_col).agg(**agg).reset_index()
    g = g.rename(columns={group_col: "Grupo"})
    g = g.sort_values("__count", ascending=False)

    return g


# ==========================================================
# TENDÊNCIA COM DIAS SEM OCORRÊNCIA
# ==========================================================
def tendencia_por_data_fill(df: pd.DataFrame) -> pd.DataFrame:
    date_col = _get_primary_date_col(df)
    if not date_col:
        return pd.DataFrame()

    temp = df.copy()
    temp[date_col] = pd.to_datetime(temp[date_col].astype(str).str.strip(), errors="coerce")
    temp = temp.dropna(subset=[date_col])

    if temp.empty:
        return pd.DataFrame()

    temp["dia"] = temp[date_col].dt.normalize()
    g = temp.groupby("dia").size().reset_index(name="declarações").sort_values("dia")

    min_d = g["dia"].min()
    max_d = g["dia"].max()
    full = pd.DataFrame({"dia": pd.date_range(min_d, max_d, freq="D")})

    g = full.merge(g, on="dia", how="left")
    g["declarações"] = g["declarações"].fillna(0).astype(int)

    return g
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
# UI
# ==========================================================
st.title("⚖️ Painel de Declarações de Arbitragem")
st.caption("Coordenador + valores exibidos em R$ (sem conversão) + data principal por fechamento.")

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
# ✅ CONTROLES DO RANKING NA BARRA LATERAL
# ---------------------------
st.sidebar.divider()
st.sidebar.subheader("Rankings")

ranking_options = [
    COL_COORDENADOR,
    COL_BASE_RESP,
    COL_BASE_REM,
    COL_BASE_FIN,
    COL_REG_RESP,
    COL_REG_REM,
    COL_ANOM_PRIM,
    COL_ANOM_SEC,
    COL_PRODUTO,
    COL_DECLARANTE,
]

ranking_group = st.sidebar.selectbox(
    "Agrupar por",
    ranking_options,
    index=0
)

ranking_top_n = st.sidebar.slider(
    "Top N",
    5, 50, 15, step=5
)

# ---------------------------
# Filtros gerais
# ---------------------------
df_f = apply_filters(df)

# ---------------------------
# Tabs
# ---------------------------
tab1, tab2, tab3 = st.tabs(["Visão Geral", "Rankings", "Detalhe"])

# ==========================================================
# VISÃO GERAL
# ==========================================================
with tab1:
    st.subheader("KPIs")
    render_kpis(df_f)

    st.divider()
    st.subheader("Tendência de declarações (por data principal)")

    trend = tendencia_por_data_fill(df_f)
    if trend.empty:
        st.info("Sem data válida para tendência (fechamento/declaração).")
    else:
        fig = px.bar(
            trend,
            x="dia",
            y="declarações",
            text="declarações",
            title="Declarações por dia (com dias sem ocorrência)"
        )
        fig.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Distribuição operacional (Top 10)")

    dims_avail = [c for c in ranking_options if c in df_f.columns]
    if dims_avail:
        dim = st.selectbox("Escolha a dimensão", dims_avail, index=0)

        dist = (
            df_f.groupby(dim)
            .size()
            .reset_index(name="qtd")
            .sort_values("qtd", ascending=False)
            .head(10)
        )

        fig2 = px.bar(dist, x=dim, y="qtd", text="qtd", title=f"Top 10 — {dim}")
        fig2.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.caption("Sem dimensão disponível para distribuição.")

# ==========================================================
# RANKINGS
# ==========================================================
with tab2:
    st.subheader("Rankings operacionais")

    rank = ranking_simple(df_f, ranking_group)

    if rank.empty:
        st.info("Não foi possível gerar ranking para este agrupamento.")
    else:
        show = rank.head(ranking_top_n).copy()
        for c in ["valor_total", "valor_medio"]:
            if c in show.columns:
                show[c] = show[c].round(2)

        st.dataframe(show, use_container_width=True)

        fig = px.bar(show, x="Grupo", y="__count",
                     title=f"Top {ranking_top_n} por volume — {ranking_group}")
        st.plotly_chart(fig, use_container_width=True)

# ==========================================================
# DETALHE
# ==========================================================
with tab3:
    st.subheader("Dados filtrados")

    all_cols = df_f.columns.tolist()

    default_cols = [c for c in [
        COL_COORDENADOR,
        COL_NUMERO, COL_REMESSA, COL_PRODUTO,
        COL_ANOM_PRIM, COL_ANOM_SEC,
        COL_BASE_REM, COL_REG_REM,
        COL_BASE_RESP, COL_REG_RESP,
        COL_BASE_FIN,
        COL_DATA_FECHAMENTO,
        COL_DATA_DECLARACAO,
        COL_VALOR_RS,
        COL_VALOR_NOVO,
        COL_VALOR_ANTIGO,
        "__valor_base",
        COL_PESO,
        "Origem da Solicitação",
        "Origem do Pedido",
        "Fonte",
        COL_STATUS_ARB,
        COL_TIPO_DECISAO,
        "__arquivo_origem"
    ] if c in all_cols]

    cols_sel = st.multiselect("Colunas para exibição", options=all_cols, default=default_cols)

    st.dataframe(df_f[cols_sel] if cols_sel else df_f, use_container_width=True)

    st.divider()
    st.subheader("Download")

    st.download_button(
        label="⬇️ Baixar Excel filtrado",
        data=to_excel_bytes(df_f),
        file_name="arbitragem_filtrada.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with st.expander("ℹ️ Notas rápidas"):
    st.write(
        "- Removido **Atraso médio (dias)** de KPIs, filtros, rankings e colunas padrão.\n"
        "- Tendência agora **preenche dias sem ocorrência com 0**.\n"
        "- Ranking segue controlado na **barra lateral**."
    )
