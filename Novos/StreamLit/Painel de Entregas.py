# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px


# ==========================================================
# CONFIG DO APP
# ==========================================================
st.set_page_config(
    page_title="Painel de Entregas",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ==========================================================
# CSS (VISUAL)
# ==========================================================
def inject_css():
    st.markdown(
        """
        <style>
        /* =========================
           Layout geral
        ========================== */
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2.2rem;
            max-width: 1600px;
        }
        [data-testid="stSidebar"] {
            padding-top: 1.0rem;
        }

        /* =========================
           Tipografia
        ========================== */
        h1, h2, h3 {
            letter-spacing: -0.3px;
        }

        /* =========================
           Cards básicos
        ========================== */
        .section-card {
            background: rgba(255,255,255,0.035);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 16px;
            padding: 16px 18px;
            margin: 8px 0 16px 0;
        }

        .section-title {
            font-size: 1.05rem;
            font-weight: 600;
            opacity: 0.95;
            margin-bottom: 4px;
        }

        .section-sub {
            opacity: 0.75;
            font-size: 0.92rem;
            margin: 0 0 6px 0;
        }

        /* =========================
           Badges / Chips
        ========================== */
        .badge {
            display: inline-block;
            padding: 3px 9px;
            border-radius: 999px;
            font-size: 11px;
            opacity: 0.85;
            border: 1px solid rgba(255,255,255,0.12);
            margin-right: 6px;
            margin-top: 6px;
        }

        /* =========================
           KPIs
        ========================== */
        .kpi-wrap {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.07);
            border-radius: 16px;
            padding: 10px 12px 2px 12px;
            margin-bottom: 10px;
        }

        [data-testid="stMetricValue"] {
            font-size: 1.32rem;
        }
        [data-testid="stMetricLabel"] {
            opacity: 0.9;
        }

        /* =========================
           Tabelas
        ========================== */
        .stDataFrame {
            border-radius: 12px;
            overflow: hidden;
        }

        /* =========================
           Separadores suaves
        ========================== */
        hr {
            border-top: 1px solid rgba(255,255,255,0.07);
        }
        </style>
        """,
        unsafe_allow_html=True
    )


inject_css()


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

COL_COORDENADOR = "Coordenador"
COL_REGIONAL_ENTREGA = "Regional de entrega"

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

NAO_COLS = [
    "Insucessos de entrega - cliente",
    "Não entregues - pct prob (outros)",
    "Não entregues - sem pct prob",
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


def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
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


def _consolidate_duplicate_numeric_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Se existirem colunas duplicadas com o mesmo nome (muito comum em Excel),
    consolida somando linha a linha e mantém apenas uma coluna com o nome original.
    Isso evita o erro do Pandas no apply e preserva o valor total.
    """
    for col in cols:
        if col in df.columns:
            mask = [c == col for c in df.columns]
            if sum(mask) > 1:
                dup_df = df.loc[:, mask].copy()
                dup_num = dup_df.apply(pd.to_numeric, errors="coerce").fillna(0)
                summed = dup_num.sum(axis=1)

                # remove todas as colunas duplicadas com este nome
                df = df.loc[:, [not m for m in mask]]
                # recria a coluna consolidada
                df[col] = summed

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


def _fmt_int(x: int) -> str:
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return "0"


def _list_excel_files(folder: str) -> List[Path]:
    p = Path(folder)
    if not p.exists() or not p.is_dir():
        return []

    files: List[Path] = []
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


def _sum_if_exists(df: pd.DataFrame, col: str) -> int:
    if col in df.columns:
        return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
    return 0


def _sum_many(df: pd.DataFrame, cols: List[str]) -> int:
    s = 0
    for c in cols:
        s += _sum_if_exists(df, c)
    return s


def _has_cols(df: pd.DataFrame, cols: List[str]) -> bool:
    return all(c in df.columns for c in cols)
# ==========================================================
# LEITURA (CACHE)
# ==========================================================
@st.cache_data(show_spinner=False)
def load_excel_file(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)

    # 1) limpa nomes
    df = _strip_columns(df)

    # 2) renomeia chinês -> PT
    df = _safe_rename_columns(df)

    # 3) datas
    df = _ensure_datetime(df, COL_DATA)

    # 4) candidatos numéricos
    numeric_candidates = [
        COL_QTD_A_ENTREGAR, COL_QTD_1_TENT, COL_QTD_PRAZO, COL_QTD_ATRASO,
        *NAO_COLS,
        "Assinadas até 15h (Qtd)",
    ]

    # 5) consolida duplicadas numéricas ANTES de coerção
    df = _consolidate_duplicate_numeric_cols(df, numeric_candidates)

    # 6) coerção final
    df = _coerce_numeric(df, numeric_candidates)

    df["__arquivo_origem"] = Path(path).name
    return df


@st.cache_data(show_spinner=False)
def load_from_folder_cached(folder: str, signature: Tuple[Tuple[str, float], ...]) -> pd.DataFrame:
    _ = signature  # usado somente para invalidar o cache

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
    _ = file_sizes  # invalida cache quando tamanho muda
    from io import BytesIO

    dfs = []
    for name, b in zip(file_names, files_bytes):
        try:
            df = pd.read_excel(BytesIO(b), sheet_name=0)

            df = _strip_columns(df)
            df = _safe_rename_columns(df)
            df = _ensure_datetime(df, COL_DATA)

            numeric_candidates = [
                COL_QTD_A_ENTREGAR, COL_QTD_1_TENT, COL_QTD_PRAZO, COL_QTD_ATRASO,
                *NAO_COLS,
                "Assinadas até 15h (Qtd)",
            ]

            df = _consolidate_duplicate_numeric_cols(df, numeric_candidates)
            df = _coerce_numeric(df, numeric_candidates)

            df["__arquivo_origem"] = name
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# ==========================================================
# FILTROS (COM SESSÃO)
# ==========================================================
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### Filtros")

    # ---------------------------
    # PERÍODO
    # ---------------------------
    with st.sidebar.expander("Período", expanded=True):
        if COL_DATA in df.columns:
            min_d = df[COL_DATA].min()
            max_d = df[COL_DATA].max()

            if not pd.isna(min_d) and not pd.isna(max_d):
                key_period = "f_period"
                default_value = (min_d.date(), max_d.date())
                period = st.date_input(
                    "Selecione o intervalo",
                    value=st.session_state.get(key_period, default_value),
                    min_value=min_d.date(),
                    max_value=max_d.date(),
                    key=key_period
                )
                if isinstance(period, tuple) and len(period) == 2:
                    d1, d2 = period
                    df = df[df[COL_DATA].between(pd.to_datetime(d1), pd.to_datetime(d2))]

    # ---------------------------
    # DIMENSÕES
    # ---------------------------
    filter_cols = [
        "Regional Remetente",
        "Base remetente",
        COL_REGIONAL_ENTREGA,
        "Nome SC Destino/HUB",
        COL_COORDENADOR,
        COL_BASE_ENTREGA,
        "Tipo de produto",
        "Turno de linha secundária",
    ]

    with st.sidebar.expander("Dimensões", expanded=True):
        for col in filter_cols:
            if col in df.columns:
                options = sorted(
                    [x for x in df[col].dropna().unique().tolist() if str(x).strip() != ""]
                )
                if options:
                    key = f"f_{col}"
                    sel = st.multiselect(
                        col,
                        options,
                        default=st.session_state.get(key, []),
                        key=key
                    )
                    if sel:
                        df = df[df[col].isin(sel)]

    # ---------------------------
    # AÇÕES
    # ---------------------------
    with st.sidebar.expander("Ações", expanded=False):
        if st.button("Limpar filtros"):
            keys_to_drop = [k for k in st.session_state.keys() if k.startswith("f_")]
            for k in keys_to_drop:
                st.session_state.pop(k, None)
            st.rerun()

    return df
# ==========================================================
# PERÍODO SELECIONADO
# ==========================================================
def _get_selected_period_from_state(df: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    key_period = "f_period"
    if COL_DATA not in df.columns:
        return None

    min_d = df[COL_DATA].min()
    max_d = df[COL_DATA].max()
    if pd.isna(min_d) or pd.isna(max_d):
        return None

    val = st.session_state.get(key_period, (min_d.date(), max_d.date()))
    if isinstance(val, tuple) and len(val) == 2:
        d1, d2 = val
        return (pd.to_datetime(d1), pd.to_datetime(d2))
    return None


# ==========================================================
# KPIs
# ==========================================================
def compute_totals(df: pd.DataFrame) -> Dict[str, int]:
    totals = {
        "a_entregar": _sum_if_exists(df, COL_QTD_A_ENTREGAR),
        "tent_1": _sum_if_exists(df, COL_QTD_1_TENT),
        "prazo": _sum_if_exists(df, COL_QTD_PRAZO),
        "atraso": _sum_if_exists(df, COL_QTD_ATRASO),
        "nao_entregues": _sum_many(df, NAO_COLS),
        "assinadas_15h": _sum_if_exists(df, "Assinadas até 15h (Qtd)"),
    }
    return totals


def build_kpis(df_all: pd.DataFrame, df_filtered: pd.DataFrame):
    totals = compute_totals(df_filtered)

    a_entregar = totals["a_entregar"]
    tent_1 = totals["tent_1"]
    prazo = totals["prazo"]
    atraso = totals["atraso"]
    nao_entregues = totals["nao_entregues"]
    assinadas_15h = totals["assinadas_15h"]

    taxa_1_tent = _rate(tent_1, a_entregar)
    taxa_prazo = _rate(prazo, a_entregar)
    taxa_atraso = _rate(atraso, a_entregar)
    taxa_nao = _rate(nao_entregues, a_entregar)
    taxa_15h = _rate(assinadas_15h, a_entregar)

    # ---------------------------
    # COMPARATIVO (PERÍODO ANTERIOR)
    # ---------------------------
    delta_map: Dict[str, float] = {}

    sel_period = _get_selected_period_from_state(df_all)
    if sel_period and COL_DATA in df_all.columns:
        d1, d2 = sel_period
        days = max((d2 - d1).days, 0) + 1
        prev_end = d1 - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=days - 1)

        df_prev = df_all[df_all[COL_DATA].between(prev_start, prev_end)].copy()

        prev_totals = compute_totals(df_prev)
        prev_a = prev_totals["a_entregar"]

        prev_taxa_prazo = _rate(prev_totals["prazo"], prev_a)
        prev_taxa_1 = _rate(prev_totals["tent_1"], prev_a)
        prev_taxa_atraso = _rate(prev_totals["atraso"], prev_a)
        prev_taxa_nao = _rate(prev_totals["nao_entregues"], prev_a)

        delta_map = {
            "a_entregar": a_entregar - prev_a,
            "taxa_prazo": taxa_prazo - prev_taxa_prazo,
            "taxa_1": taxa_1_tent - prev_taxa_1,
            "taxa_atraso": taxa_atraso - prev_taxa_atraso,
            "taxa_nao": taxa_nao - prev_taxa_nao,
        }

    # ---------------------------
    # RENDER KPIs
    # ---------------------------
    st.markdown('<div class="kpi-wrap">', unsafe_allow_html=True)

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    c1.metric(
        "Qtd a entregar",
        _fmt_int(a_entregar),
        delta=_fmt_int(delta_map["a_entregar"]) if "a_entregar" in delta_map else None
    )
    c2.metric("1ª tentativa (Qtd)", _fmt_int(tent_1))
    c3.metric(
        "1ª tentativa (Taxa)",
        _format_pct(taxa_1_tent),
        delta=_format_pct(delta_map["taxa_1"]) if "taxa_1" in delta_map else None
    )
    c4.metric(
        "SLA global (No prazo)",
        _format_pct(taxa_prazo),
        delta=_format_pct(delta_map["taxa_prazo"]) if "taxa_prazo" in delta_map else None
    )
    c5.metric(
        "Atraso (Taxa)",
        _format_pct(taxa_atraso),
        delta=_format_pct(delta_map["taxa_atraso"]) if "taxa_atraso" in delta_map else None
    )
    c6.metric(
        "Não entregues (Taxa)",
        _format_pct(taxa_nao),
        delta=_format_pct(delta_map["taxa_nao"]) if "taxa_nao" in delta_map else None
    )

    if "Assinadas até 15h (Qtd)" in df_filtered.columns:
        st.divider()
        c7, c8, c9 = st.columns(3)
        c7.metric("Assinadas até 15h (Qtd)", _fmt_int(assinadas_15h))
        c8.metric("Assinadas até 15h (Taxa calc.)", _format_pct(taxa_15h))
        c9.metric("Não entregues (Qtd)", _fmt_int(nao_entregues))

    st.markdown("</div>", unsafe_allow_html=True)


# ==========================================================
# AGREGAÇÃO GENÉRICA POR DIMENSÃO (BLINDADA)
# ==========================================================
def agg_por_dimensao(df: pd.DataFrame, dim_col: str) -> pd.DataFrame:
    if dim_col not in df.columns:
        return pd.DataFrame()

    base_cols = [COL_QTD_A_ENTREGAR]
    opt_cols = [COL_QTD_1_TENT, COL_QTD_PRAZO, COL_QTD_ATRASO, "Assinadas até 15h (Qtd)"] + NAO_COLS

    cols_exist = [c for c in base_cols + opt_cols if c in df.columns]
    cols_exist = list(dict.fromkeys(cols_exist))  # remove duplicadas na lista

    if COL_QTD_A_ENTREGAR not in cols_exist:
        return pd.DataFrame()

    g = df.groupby(dim_col, dropna=False)[cols_exist].sum().reset_index()
    g[dim_col] = g[dim_col].fillna(f"SEM {dim_col.upper()}")

    # métricas calculadas
    if COL_QTD_PRAZO in g.columns:
        g["SLA (%)"] = g.apply(
            lambda r: _rate(r.get(COL_QTD_PRAZO, 0), r.get(COL_QTD_A_ENTREGAR, 0)),
            axis=1
        )
    else:
        g["SLA (%)"] = 0.0

    if COL_QTD_1_TENT in g.columns:
        g["Taxa 1ª tentativa (calc.)"] = g.apply(
            lambda r: _rate(r.get(COL_QTD_1_TENT, 0), r.get(COL_QTD_A_ENTREGAR, 0)),
            axis=1
        )

    if COL_QTD_ATRASO in g.columns:
        g["Taxa atraso (calc.)"] = g.apply(
            lambda r: _rate(r.get(COL_QTD_ATRASO, 0), r.get(COL_QTD_A_ENTREGAR, 0)),
            axis=1
        )

    # não entregues consolidados
    if any(c in g.columns for c in NAO_COLS):

        def _to_int_safe(v) -> int:
            if isinstance(v, pd.Series):
                return int(pd.to_numeric(v, errors="coerce").fillna(0).sum())
            v = pd.to_numeric(v, errors="coerce")
            return int(0 if pd.isna(v) else v)

        def _sum_nao(row: pd.Series) -> int:
            s = 0
            for c in NAO_COLS:
                if c in row.index:
                    s += _to_int_safe(row.get(c, 0))
            return s

        g["Qtd não entregues (calc.)"] = g.apply(_sum_nao, axis=1)
        g["Taxa não entregues (calc.)"] = g.apply(
            lambda r: _rate(r.get("Qtd não entregues (calc.)", 0), r.get(COL_QTD_A_ENTREGAR, 0)),
            axis=1
        )

    return g


# ==========================================================
# GRÁFICOS
# ==========================================================
def render_charts_time(df: pd.DataFrame):
    needed = [COL_DATA, COL_QTD_A_ENTREGAR]
    if any(c not in df.columns for c in needed):
        st.info("Sem dados suficientes para gráfico por data.")
        return

    cols_to_plot = [COL_QTD_A_ENTREGAR]
    if COL_QTD_PRAZO in df.columns:
        cols_to_plot.append(COL_QTD_PRAZO)
    if COL_QTD_ATRASO in df.columns:
        cols_to_plot.append(COL_QTD_ATRASO)

    daily = df.groupby(COL_DATA)[cols_to_plot].sum().reset_index()
    long = daily.melt(id_vars=[COL_DATA], value_vars=cols_to_plot, var_name="Métrica", value_name="Qtd")

    fig = px.line(
        long,
        x=COL_DATA,
        y="Qtd",
        color="Métrica",
        markers=True,
        title="Tendência por data"
    )
    fig.update_layout(legend_title_text="", margin=dict(l=10, r=10, t=60, b=10))
    st.plotly_chart(fig, use_container_width=True)


def render_composition(df: pd.DataFrame):
    if COL_QTD_A_ENTREGAR not in df.columns:
        return

    total_a = _sum_if_exists(df, COL_QTD_A_ENTREGAR)
    total_p = _sum_if_exists(df, COL_QTD_PRAZO)
    total_at = _sum_if_exists(df, COL_QTD_ATRASO)
    total_nao = _sum_many(df, NAO_COLS)

    if total_a <= 0 and (total_p + total_at + total_nao) <= 0:
        st.info("Sem dados suficientes para composição.")
        return

    comp = pd.DataFrame({
        "Status": ["No prazo", "Atraso", "Não entregues"],
        "Qtd": [total_p, total_at, total_nao]
    })

    fig = px.pie(comp, names="Status", values="Qtd", hole=0.55, title="Composição de entregas")
    fig.update_layout(margin=dict(l=10, r=10, t=60, b=10))
    st.plotly_chart(fig, use_container_width=True)


def render_top_bottom_by_dim(df_dim: pd.DataFrame, dim_col: str, top_n: int = 15):
    if df_dim.empty or dim_col not in df_dim.columns or "SLA (%)" not in df_dim.columns:
        return

    col1, col2 = st.columns(2)
    top = df_dim.sort_values("SLA (%)", ascending=False).head(top_n)
    bottom = df_dim.sort_values("SLA (%)", ascending=True).head(top_n)

    with col1:
        fig = px.bar(top, x=dim_col, y="SLA (%)", title=f"Top {top_n} SLA - {dim_col}")
        fig.update_layout(xaxis_title="", yaxis_title="SLA", margin=dict(l=10, r=10, t=60, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(bottom, x=dim_col, y="SLA (%)", title=f"Bottom {top_n} SLA - {dim_col}")
        fig.update_layout(xaxis_title="", yaxis_title="SLA", margin=dict(l=10, r=10, t=60, b=10))
        st.plotly_chart(fig, use_container_width=True)


def render_volume_vs_sla(df_dim: pd.DataFrame, dim_col: str):
    if df_dim.empty:
        return
    if COL_QTD_A_ENTREGAR not in df_dim.columns or "SLA (%)" not in df_dim.columns:
        return

    fig = px.scatter(
        df_dim,
        x=COL_QTD_A_ENTREGAR,
        y="SLA (%)",
        hover_name=dim_col if dim_col in df_dim.columns else None,
        title=f"Volume x SLA - {dim_col}"
    )
    fig.update_layout(xaxis_title="Qtd a entregar", yaxis_title="SLA", margin=dict(l=10, r=10, t=60, b=10))
    st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# ALERTAS
# ==========================================================
def build_alerts_base(df_all: pd.DataFrame, df_filtered: pd.DataFrame, sla_min: float) -> Dict[str, pd.DataFrame]:
    if not _has_cols(df_filtered, [COL_BASE_ENTREGA, COL_QTD_A_ENTREGAR]):
        return {"below_sla": pd.DataFrame(), "drop_vs_prev": pd.DataFrame(), "cur_base": pd.DataFrame(), "prev_base": pd.DataFrame()}

    cur_base = agg_por_dimensao(df_filtered, COL_BASE_ENTREGA)
    if cur_base.empty:
        return {"below_sla": pd.DataFrame(), "drop_vs_prev": pd.DataFrame(), "cur_base": pd.DataFrame(), "prev_base": pd.DataFrame()}

    below_sla = cur_base[cur_base["SLA (%)"] < sla_min].copy().sort_values("SLA (%)", ascending=True)

    drop_vs_prev = pd.DataFrame()
    prev_base = pd.DataFrame()

    sel_period = _get_selected_period_from_state(df_all)
    if sel_period and COL_DATA in df_all.columns:
        d1, d2 = sel_period
        days = max((d2 - d1).days, 0) + 1
        prev_end = d1 - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=days - 1)

        df_prev = df_all[df_all[COL_DATA].between(prev_start, prev_end)].copy()
        prev_base = agg_por_dimensao(df_prev, COL_BASE_ENTREGA)

        if not prev_base.empty:
            m = cur_base[[COL_BASE_ENTREGA, COL_QTD_A_ENTREGAR, "SLA (%)"]].merge(
                prev_base[[COL_BASE_ENTREGA, "SLA (%)"]],
                on=COL_BASE_ENTREGA,
                how="left",
                suffixes=("", "_prev")
            )
            m["Δ SLA vs período anterior"] = m["SLA (%)"] - m["SLA (%)_prev"].fillna(0.0)
            drop_vs_prev = m[m["Δ SLA vs período anterior"] < 0].copy().sort_values("Δ SLA vs período anterior", ascending=True)

    return {"below_sla": below_sla, "drop_vs_prev": drop_vs_prev, "cur_base": cur_base, "prev_base": prev_base}


# ==========================================================
# EXCEL DOWNLOAD
# ==========================================================
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
# HEADER DO APP
# ==========================================================
def render_header():
    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Painel de Entregas</div>
            <div class="section-sub">
                Monitoramento operacional com SLA por Base, Coordenador e Regional, além de alertas por risco.
            </div>
            <span class="badge">Python</span>
            <span class="badge">Streamlit</span>
            <span class="badge">SLA ponderado</span>
            <span class="badge">Comparativo de período</span>
            <span class="badge">Alertas</span>
        </div>
        """,
        unsafe_allow_html=True
    )


# ==========================================================
# APP
# ==========================================================
render_header()

# ---------------------------
# SIDEBAR - FONTE
# ---------------------------
st.sidebar.markdown("## Fonte dos dados")

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
        if st.button("Escolher pasta"):
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

    st.sidebar.success(f"{len(files)} arquivo(s) encontrado(s).")
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
# SIDEBAR - ALERTAS CONFIG
# ---------------------------
st.sidebar.markdown("## Parâmetros")
with st.sidebar.expander("Regras de alerta", expanded=True):
    sla_min = st.slider("SLA mínimo aceitável", 0.50, 0.99, 0.90, 0.01)

# ---------------------------
# FILTROS
# ---------------------------
df_f = apply_filters(df)

# ---------------------------
# TABS
# ---------------------------
tab1, tab2, tab3, tab4 = st.tabs(["Visão Geral", "SLA por Base", "SLA por Dimensão", "Alertas"])


# ==========================================================
# TAB 1 - VISÃO GERAL
# ==========================================================
with tab1:
    left, right = st.columns([2.2, 1.2])

    with left:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">KPIs</div>', unsafe_allow_html=True)
        build_kpis(df, df_f)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Tendência</div>', unsafe_allow_html=True)
        render_charts_time(df_f)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Composição</div>', unsafe_allow_html=True)
        render_composition(df_f)
        st.markdown("</div>", unsafe_allow_html=True)

        totals = compute_totals(df_f)
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Resumo rápido</div>', unsafe_allow_html=True)

        st.write(f"Qtd a entregar: {_fmt_int(totals['a_entregar'])}")
        st.write(f"No prazo: {_fmt_int(totals['prazo'])}")
        st.write(f"Atraso: {_fmt_int(totals['atraso'])}")
        st.write(f"Não entregues: {_fmt_int(totals['nao_entregues'])}")
        if totals["a_entregar"] > 0:
            st.write(f"SLA calc.: {_format_pct(_rate(totals['prazo'], totals['a_entregar']))}")

        st.markdown("</div>", unsafe_allow_html=True)


# ==========================================================
# TAB 2 - SLA POR BASE
# ==========================================================
with tab2:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">SLA por Base de entrega</div>', unsafe_allow_html=True)

    base_df = agg_por_dimensao(df_f, COL_BASE_ENTREGA)

    if base_df.empty:
        st.info("Preciso das colunas mínimas: 'Base de entrega' e 'Qtd a entregar' (ideal também 'Qtd entregas no prazo').")
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        ctrl1, ctrl2, ctrl3 = st.columns([1.3, 1.0, 1.2])
        with ctrl1:
            sort_by = st.selectbox(
                "Ordenar por",
                ["SLA (%)", "Qtd a entregar", "Taxa 1ª tentativa (calc.)", "Taxa atraso (calc.)", "Taxa não entregues (calc.)"],
                index=0
            )
        with ctrl2:
            ascending = st.checkbox("Ordem crescente", value=False)
        with ctrl3:
            top_n = st.slider("Top/Bottom N bases", 5, 50, 15, step=1)

        if sort_by not in base_df.columns:
            sort_by = "SLA (%)"

        base_df = base_df.sort_values(sort_by, ascending=ascending)

        view = base_df.copy()
        for c in ["SLA (%)", "Taxa 1ª tentativa (calc.)", "Taxa atraso (calc.)", "Taxa não entregues (calc.)"]:
            if c in view.columns:
                view[c] = view[c].apply(_format_pct)

        st.dataframe(view.head(max(top_n, 25)), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.divider()

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Top e Bottom SLA</div>', unsafe_allow_html=True)
        render_top_bottom_by_dim(agg_por_dimensao(df_f, COL_BASE_ENTREGA), COL_BASE_ENTREGA, top_n=top_n)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Volume x SLA</div>', unsafe_allow_html=True)
        render_volume_vs_sla(agg_por_dimensao(df_f, COL_BASE_ENTREGA), COL_BASE_ENTREGA)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Tabela completa</div>', unsafe_allow_html=True)
        st.dataframe(view, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)


# ==========================================================
# TAB 3 - SLA POR DIMENSÃO
# ==========================================================
with tab3:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Visão por Coordenador</div>', unsafe_allow_html=True)

    coord_df = agg_por_dimensao(df_f, COL_COORDENADOR)
    if coord_df.empty:
        st.info("Coluna 'Coordenador' não encontrada ou sem dados suficientes.")
    else:
        view = coord_df.copy()
        for c in ["SLA (%)", "Taxa 1ª tentativa (calc.)", "Taxa atraso (calc.)", "Taxa não entregues (calc.)"]:
            if c in view.columns:
                view[c] = view[c].apply(_format_pct)

        st.dataframe(view.sort_values("Qtd a entregar", ascending=False), use_container_width=True)
        st.divider()
        render_top_bottom_by_dim(coord_df, COL_COORDENADOR, top_n=10)

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Visão por Regional de entrega</div>', unsafe_allow_html=True)

    reg_df = agg_por_dimensao(df_f, COL_REGIONAL_ENTREGA)
    if reg_df.empty:
        st.info("Coluna 'Regional de entrega' não encontrada ou sem dados suficientes.")
    else:
        view = reg_df.copy()
        for c in ["SLA (%)", "Taxa 1ª tentativa (calc.)", "Taxa atraso (calc.)", "Taxa não entregues (calc.)"]:
            if c in view.columns:
                view[c] = view[c].apply(_format_pct)

        st.dataframe(view.sort_values("Qtd a entregar", ascending=False), use_container_width=True)
        st.divider()
        render_top_bottom_by_dim(reg_df, COL_REGIONAL_ENTREGA, top_n=10)

    st.markdown("</div>", unsafe_allow_html=True)


# ==========================================================
# TAB 4 - ALERTAS
# ==========================================================
with tab4:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Resumo de risco</div>', unsafe_allow_html=True)

    alerts = build_alerts_base(df, df_f, sla_min=sla_min)
    below_sla = alerts["below_sla"]
    drop_vs_prev = alerts["drop_vs_prev"]

    c1, c2, c3 = st.columns(3)
    c1.metric("SLA mínimo configurado", _format_pct(sla_min))
    c2.metric("Bases abaixo do SLA", _fmt_int(len(below_sla) if not below_sla.empty else 0))
    c3.metric("Bases com queda vs período anterior", _fmt_int(len(drop_vs_prev) if not drop_vs_prev.empty else 0))

    st.markdown("</div>", unsafe_allow_html=True)

    # ALERTA 1
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Bases abaixo do SLA mínimo</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Ordem crescente de SLA (piores primeiro).</div>', unsafe_allow_html=True)

    if below_sla.empty:
        st.success("Nenhuma base abaixo do SLA mínimo com os filtros atuais.")
    else:
        view = below_sla.copy()
        if "SLA (%)" in view.columns:
            view["SLA (%)"] = view["SLA (%)"].apply(_format_pct)
        if "Taxa 1ª tentativa (calc.)" in view.columns:
            view["Taxa 1ª tentativa (calc.)"] = view["Taxa 1ª tentativa (calc.)"].apply(_format_pct)
        if "Taxa atraso (calc.)" in view.columns:
            view["Taxa atraso (calc.)"] = view["Taxa atraso (calc.)"].apply(_format_pct)
        if "Taxa não entregues (calc.)" in view.columns:
            view["Taxa não entregues (calc.)"] = view["Taxa não entregues (calc.)"].apply(_format_pct)

        st.dataframe(view, use_container_width=True)

        worst = below_sla.head(15).copy()
        fig = px.bar(worst, x=COL_BASE_ENTREGA, y="SLA (%)", title="Piores bases (SLA)")
        fig.update_layout(xaxis_title="", yaxis_title="SLA", margin=dict(l=10, r=10, t=60, b=10))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # ALERTA 2
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Queda de SLA vs período anterior equivalente</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Este bloco só faz sentido quando o filtro de período está ativo.</div>', unsafe_allow_html=True)

    if drop_vs_prev.empty:
        st.info("Sem quedas detectadas (ou não há período anterior comparável).")
    else:
        view = drop_vs_prev.copy()
        view["SLA (%)"] = view["SLA (%)"].apply(_format_pct)
        view["SLA (%)_prev"] = view["SLA (%)_prev"].apply(_format_pct)
        view["Δ SLA vs período anterior"] = view["Δ SLA vs período anterior"].apply(_format_pct)

        keep = [
            COL_BASE_ENTREGA,
            COL_QTD_A_ENTREGAR,
            "SLA (%)",
            "SLA (%)_prev",
            "Δ SLA vs período anterior",
        ]
        keep = [c for c in keep if c in view.columns]

        st.dataframe(view[keep], use_container_width=True)

        chart_df = drop_vs_prev.head(20).copy()
        fig = px.bar(chart_df, x=COL_BASE_ENTREGA, y="Δ SLA vs período anterior", title="Maiores quedas de SLA")
        fig.update_layout(xaxis_title="", yaxis_title="Δ SLA", margin=dict(l=10, r=10, t=60, b=10))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)


# ==========================================================
# DETALHE + DOWNLOAD
# ==========================================================
with st.expander("Detalhe dos dados filtrados"):
    all_cols = df_f.columns.tolist()
    default_cols = [c for c in [COL_DATA] + DIMENSIONS_DEFAULT + [
        COL_QTD_A_ENTREGAR, COL_QTD_1_TENT, COL_QTD_PRAZO, COL_QTD_ATRASO,
        "Assinadas até 15h (Qtd)", "Assinadas até 15h (Taxa)",
        *NAO_COLS,
        "__arquivo_origem"
    ] if c in all_cols]

    cols_sel = st.multiselect(
        "Colunas para exibição",
        options=all_cols,
        default=default_cols
    )

    st.dataframe(df_f[cols_sel] if cols_sel else df_f, use_container_width=True)

    st.download_button(
        label="Baixar Excel filtrado",
        data=to_excel_bytes(df_f),
        file_name="entregas_filtradas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ==========================================================
# OBSERVAÇÕES
# ==========================================================
with st.expander("Observações"):
    st.write(
        "- Todos os SLAs são ponderados por volume: soma(no prazo) / soma(a entregar).\n"
        "- Colunas numéricas duplicadas no Excel são consolidadas na leitura para evitar erro e preservar totais.\n"
        "- A aba Alertas inclui comparação com período anterior equivalente quando o filtro de datas está ativo.\n"
    )
