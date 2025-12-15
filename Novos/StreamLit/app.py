# app.py - Versão Completa (Responsiva - SQL First + Cast seguro)
# -*- coding: utf-8 -*-

import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import inspect, text
import traceback
from typing import Dict, List, Optional, Tuple, Any

from db import get_engine

# ==========================================================
# CONFIGURAÇÕES
# ==========================================================
CONFIG = {
    "PAGE_TITLE": "Sem Movimentação - PostgreSQL",
    "PAGE_ICON": "📦",
    "THEME": {
        "primaryColor": "#1f77b4",
        "backgroundColor": "#0e1117",
        "secondaryBackgroundColor": "#1e213a",
        "textColor": "#ffffff",
        "font": "sans serif",
    },
    "COLUNAS_SEM_MOV": {
        "pedido": ["número de pedido", "numero de pedido", "运单号"],
        "qtd": ["pedidos件量", "pedidos", "件量"],
        "reg_remet": ["regional remetente", "寄件代理区"],
        "base_remet": ["nome da base remetente", "寄件网点名称"],
        "reg_recente": ["regional mais recente", "最新操作代理区"],
        "base_recente": ["nome da base mais recente", "最新操作机构名称"],
        "tipo_ult_op": ["tipo da última operação", "tipo da ultima operacao", "最新操作类型"],
        "hora_ult": ["horário da última operação", "horario da ultima operacao", "最新操作时间"],
        "dias": ["dias sem mov", "dias_sem_mov", "断更天数"],
        "unid_resp": ["unidade responsável", "unidade_responsavel", "责任机构"],
        "reg_resp": ["regional responsável", "regional_responsavel", "责任所属代理区"],
        "reg_dest": ["regional destino", "目的代理区"],
        "est_dest": ["estado de destino", "目的州"],
        "base_entrega": ["base de entrega", "base_de_entrega", "派件网点"],
        "tipo_prod": ["tipo de produto", "产品类型"],
        "cliente": ["nome cliente", "客户简称"],
        "nome_prob": ["nome de pacote problemático", "nome_de_pacote_problematico", "问题件名称"],
        "aging": ["aging", "超时类型"],
    },
    "CRITICOS": {"dias_crit_1": 5, "dias_crit_2": 10, "dias_crit_3": 20},
    "VISUAIS": {
        "card_bg": "rgba(255,255,255,0.03)",
        "card_border": "rgba(255,255,255,0.12)",
        "success_color": "#10b981",
        "warning_color": "#f59e0b",
        "danger_color": "#ef4444",
        "info_color": "#3b82f6",
    },
    "PERF": {
        "DISTINCT_LIMIT": 2000,
        "DETAIL_PAGE_SIZE": 2000,
        "DETAIL_MAX_EXPORT": 300000,
    },
}

SCHEMA = "public"

# ==========================================================
# APP CONFIG
# ==========================================================
st.set_page_config(
    page_title=CONFIG["PAGE_TITLE"],
    page_icon=CONFIG["PAGE_ICON"],
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    f"""
    <style>
    :root {{
        --primary-color: {CONFIG["THEME"]["primaryColor"]};
        --background-color: {CONFIG["THEME"]["backgroundColor"]};
        --secondary-background: {CONFIG["THEME"]["secondaryBackgroundColor"]};
        --text-color: {CONFIG["THEME"]["textColor"]};
        --card-bg: {CONFIG["VISUAIS"]["card_bg"]};
        --card-border: {CONFIG["VISUAIS"]["card_border"]};
    }}
    .stApp {{
        background-color: var(--background-color);
        color: var(--text-color);
    }}
    .stSidebar {{
        background-color: var(--secondary-background);
    }}
    .block-container {{
        padding-top: 1.5rem;
        padding-bottom: 2.5rem;
        max-width: 1800px;
        background: transparent;
    }}
    .custom-card {{
        background: var(--card-bg, rgba(30, 41, 59, 0.6));
        border: 1px solid var(--card-border, rgba(148, 163, 184, 0.2));
        border-radius: 16px;
        padding: 20px;
        margin: 12px 0;
        backdrop-filter: blur(10px);
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }}
    .section-title {{
        font-size: 1.2rem;
        font-weight: 700;
        color: #e2e8f0;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(255, 255, 255, 0.1);
    }}
    .badge {{
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 500;
        margin-right: 8px;
        margin-top: 4px;
        border: 1px solid;
    }}
    .badge-primary {{ background: rgba(59, 130, 246, 0.2); border-color: #3b82f6; color: #dbeafe; }}
    .badge-success {{ background: rgba(16, 185, 129, 0.2); border-color: #10b981; color: #d1fae5; }}
    .badge-warning {{ background: rgba(245, 158, 11, 0.2); border-color: #f59e0b; color: #fed7aa; }}
    .badge-info    {{ background: rgba(59, 130, 246, 0.2); border-color: #3b82f6; color: #dbeafe; }}
    .pill {{
        display: inline-block;
        padding: 4px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 500;
        margin-right: 8px;
        margin-top: 4px;
    }}
    .pill-green  {{ background: rgba(16, 185, 129, 0.15); border: 1px solid rgba(16, 185, 129, 0.5); color: #10b981; }}
    .pill-yellow {{ background: rgba(245, 158, 11, 0.15); border: 1px solid rgba(245, 158, 11, 0.5); color: #f59e0b; }}
    .pill-red    {{ background: rgba(239, 68, 68, 0.15); border: 1px solid rgba(239, 68, 68, 0.5); color: #ef4444; }}
    .kpi-container {{
        background: rgba(30, 41, 59, 0.4);
        border-radius: 16px;
        padding: 16px;
        margin-bottom: 20px;
        border: 1px solid rgba(148, 163, 184, 0.2);
    }}
    .kpi-value {{
        font-size: 1.8rem;
        font-weight: 700;
        color: #e2e8f0;
        margin-bottom: 4px;
    }}
    .kpi-label {{
        font-size: 0.9rem;
        color: #94a3b8;
        margin-bottom: 8px;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ==========================================================
# UTILITÁRIOS
# ==========================================================
def format_number(num: float, decimals: int = 0) -> str:
    try:
        if pd.isna(num):
            return "0"
        num = float(num)
        if decimals == 0:
            return f"{int(num):,}".replace(",", ".")
        return f"{num:,.{decimals}f}".replace(",", ".")
    except (ValueError, TypeError):
        return "0"


def create_kpi_card(title: str, value: str, subtitle: str = "", color: str = "#3b82f6") -> str:
    return f"""
    <div class="kpi-container">
        <div class="kpi-label">{title}</div>
        <div class="kpi-value" style="color: {color};">{value}</div>
        {f'<div style="color: #94a3b8; font-size: 0.85rem;">{subtitle}</div>' if subtitle else ''}
    </div>
    """


def detect_columns_from_list(columns: List[str], patterns: List[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in columns if isinstance(c, str)}
    for pattern in patterns:
        p = pattern.lower()
        for name_low, orig in cols_lower.items():
            if p in name_low:
                return orig
    return None


def qname(name: str) -> str:
    return f'"{name}"'


def tqname(schema: str, table: str) -> str:
    return f'{qname(schema)}.{qname(table)}'


def _non_empty_where(col_expr: str) -> str:
    return f"{col_expr} IS NOT NULL AND NULLIF(TRIM(({col_expr})::text),'') IS NOT NULL"


def numeric_expr(col: str) -> str:
    """
    Cast seguro para NUMERIC.
    - Trata valores TEXT (ex.: ' 12 ', '12', '12,0')
    - Remove lixo não numérico
    - Retorna NULL se vazio
    """
    cq = qname(col)
    # replace vírgula por ponto; remove tudo que não é dígito, '.' ou '-'
    return (
        "NULLIF("
        f"regexp_replace(replace(trim(({cq})::text), ',', '.'), '[^0-9\\.\\-]+', '', 'g')"
        ", '')::numeric"
    )


# ==========================================================
# BANCO / CACHE
# ==========================================================
@st.cache_resource
def get_db_engine():
    return get_engine()


@st.cache_data(show_spinner="🔍 Carregando tabelas...")
def list_tables() -> List[str]:
    try:
        engine = get_db_engine()
        insp = inspect(engine)
        return sorted(insp.get_table_names(schema=SCHEMA))
    except Exception as e:
        st.error("❌ Erro ao listar tabelas")
        st.code(repr(e))
        return []


@st.cache_data(show_spinner="🧾 Lendo colunas da tabela...")
def get_table_columns(table_name: str) -> List[str]:
    engine = get_db_engine()
    insp = inspect(engine)
    cols = insp.get_columns(table_name, schema=SCHEMA)
    return [c["name"] for c in cols]


@st.cache_data(show_spinner="🧾 Lendo tipos das colunas...")
def get_table_coltypes(table_name: str) -> Dict[str, str]:
    engine = get_db_engine()
    insp = inspect(engine)
    cols = insp.get_columns(table_name, schema=SCHEMA)
    return {c["name"]: str(c["type"]).upper() for c in cols}


def detect_sem_mov_columns_db(table_name: str) -> Dict[str, Optional[str]]:
    columns = get_table_columns(table_name)
    return {k: detect_columns_from_list(columns, patterns) for k, patterns in CONFIG["COLUNAS_SEM_MOV"].items()}


@st.cache_data(show_spinner=False)
def sql_df(query: str, params: Dict[str, Any]) -> pd.DataFrame:
    engine = get_db_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


# ==========================================================
# WHERE BUILDER (SQL responsivo)
# ==========================================================
def build_where_sql(cols: Dict[str, Optional[str]], filters: Dict[str, Any], table_name: str) -> Tuple[str, Dict[str, Any]]:
    where_parts = ["1=1"]
    params: Dict[str, Any] = {}

    col_dias = cols.get("dias")
    if col_dias and filters.get("dias_range") is not None:
        # >>> CORREÇÃO PRINCIPAL: comparar pelo CAST NUMERIC (coluna pode ser TEXT)
        where_parts.append(f"{numeric_expr(col_dias)} BETWEEN :dias_min AND :dias_max")
        params["dias_min"] = int(filters["dias_range"][0])
        params["dias_max"] = int(filters["dias_range"][1])

    # Filtro por mês só se a coluna for timestamp/date no banco
    col_hora = cols.get("hora_ult")
    if col_hora and filters.get("meses"):
        coltypes = get_table_coltypes(table_name)
        t = coltypes.get(col_hora, "")
        if "TIMESTAMP" in t or "DATE" in t:
            mes_expr = f"to_char(date_trunc('month', {qname(col_hora)}), 'YYYY-MM')"
            where_parts.append(f"{mes_expr} = ANY(:meses)")
            params["meses"] = list(filters["meses"])

    # Multiselects (dimensões)
    for key, param_name in [
        ("unid_resp", "unids"),
        ("base_entrega", "bases"),
        ("est_dest", "ufs"),
        ("reg_resp", "regs"),
        ("aging", "aging"),
    ]:
        col = cols.get(key)
        sel = filters.get(key) or []
        if col and sel:
            where_parts.append(f"{qname(col)} = ANY(:{param_name})")
            params[param_name] = list(sel)

    return " AND ".join(where_parts), params


# ==========================================================
# PRÉ-CÁLCULOS PARA FILTROS
# ==========================================================
@st.cache_data(show_spinner=False)
def get_min_max_numeric(table_name: str, col: str) -> Tuple[int, int]:
    q = f"""
        SELECT
            COALESCE(MIN({numeric_expr(col)}), 0) AS mn,
            COALESCE(MAX({numeric_expr(col)}), 0) AS mx
        FROM {tqname(SCHEMA, table_name)}
        WHERE {numeric_expr(col)} IS NOT NULL
    """
    df = sql_df(q, {})
    mn = int(df.loc[0, "mn"]) if not df.empty else 0
    mx = int(df.loc[0, "mx"]) if not df.empty else 0
    if mn > mx:
        mn, mx = 0, 0
    return mn, mx


@st.cache_data(show_spinner=False)
def get_distinct_values(table_name: str, col: str, limit: int) -> List[str]:
    q = f"""
        SELECT DISTINCT {qname(col)}::text AS v
        FROM {tqname(SCHEMA, table_name)}
        WHERE {_non_empty_where(qname(col))}
        ORDER BY 1
        LIMIT :lim
    """
    df = sql_df(q, {"lim": int(limit)})
    return df["v"].astype(str).tolist() if not df.empty else []


@st.cache_data(show_spinner=False)
def get_distinct_months(table_name: str, col_hora: str, limit: int = 120) -> List[str]:
    coltypes = get_table_coltypes(table_name)
    t = coltypes.get(col_hora, "")
    if not ("TIMESTAMP" in t or "DATE" in t):
        return []
    mes_expr = f"to_char(date_trunc('month', {qname(col_hora)}), 'YYYY-MM')"
    q = f"""
        SELECT DISTINCT {mes_expr} AS mes
        FROM {tqname(SCHEMA, table_name)}
        WHERE {qname(col_hora)} IS NOT NULL
        ORDER BY 1 DESC
        LIMIT :lim
    """
    df = sql_df(q, {"lim": int(limit)})
    return df["mes"].astype(str).tolist() if not df.empty else []


# ==========================================================
# KPIs / GRÁFICOS (SQL)
# ==========================================================
@st.cache_data(show_spinner="📈 Calculando KPIs (SQL)...")
def query_kpis(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any]) -> Dict[str, float]:
    where_sql, params = build_where_sql(cols, filters, table_name)

    col_dias = cols.get("dias")
    col_qtd = cols.get("qtd")

    linhas_expr = "COUNT(*)::bigint"
    qtd_expr = f"COALESCE(SUM({numeric_expr(col_qtd)}),0)" if col_qtd else "COUNT(*)::bigint"
    media_dias_expr = f"COALESCE(AVG({numeric_expr(col_dias)}),0)" if col_dias else "0"
    max_dias_expr = f"COALESCE(MAX({numeric_expr(col_dias)}),0)" if col_dias else "0"

    c1 = CONFIG["CRITICOS"]["dias_crit_1"]
    c2 = CONFIG["CRITICOS"]["dias_crit_2"]
    c3 = CONFIG["CRITICOS"]["dias_crit_3"]

    if col_dias:
        crit1_expr = f"SUM(CASE WHEN {numeric_expr(col_dias)} >= {c1} THEN 1 ELSE 0 END)::bigint"
        crit2_expr = f"SUM(CASE WHEN {numeric_expr(col_dias)} >= {c2} THEN 1 ELSE 0 END)::bigint"
        crit3_expr = f"SUM(CASE WHEN {numeric_expr(col_dias)} >= {c3} THEN 1 ELSE 0 END)::bigint"
    else:
        crit1_expr = "0"
        crit2_expr = "0"
        crit3_expr = "0"

    q = f"""
        SELECT
            {linhas_expr} AS total_linhas,
            {qtd_expr} AS total_volume,
            {media_dias_expr} AS media_dias,
            {max_dias_expr} AS max_dias,
            {crit1_expr} AS crit_1,
            {crit2_expr} AS crit_2,
            {crit3_expr} AS crit_3
        FROM {tqname(SCHEMA, table_name)}
        WHERE {where_sql}
    """
    df = sql_df(q, params)
    if df.empty:
        return dict(total_linhas=0, total_volume=0, media_dias=0, max_dias=0, crit_1=0, crit_2=0, crit_3=0)

    row = df.iloc[0].to_dict()
    return {k: float(row[k]) for k in row.keys()}


@st.cache_data(show_spinner="📊 Carregando distribuição de dias (SQL)...")
def query_hist_dias(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any]) -> pd.DataFrame:
    col_dias = cols.get("dias")
    if not col_dias:
        return pd.DataFrame()

    where_sql, params = build_where_sql(cols, filters, table_name)
    dias_num = numeric_expr(col_dias)

    q = f"""
        SELECT {dias_num} AS dias, COUNT(*)::bigint AS linhas
        FROM {tqname(SCHEMA, table_name)}
        WHERE {where_sql} AND {dias_num} IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    return sql_df(q, params)


@st.cache_data(show_spinner="📈 Carregando volume por mês (SQL)...")
def query_volume_mes(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any]) -> pd.DataFrame:
    col_hora = cols.get("hora_ult")
    if not col_hora:
        return pd.DataFrame()

    coltypes = get_table_coltypes(table_name)
    t = coltypes.get(col_hora, "")
    if not ("TIMESTAMP" in t or "DATE" in t):
        return pd.DataFrame()

    where_sql, params = build_where_sql(cols, filters, table_name)
    col_qtd = cols.get("qtd")

    mes_expr = f"to_char(date_trunc('month', {qname(col_hora)}), 'YYYY-MM')"
    if col_qtd:
        y_expr = f"COALESCE(SUM({numeric_expr(col_qtd)}),0)"
        y_name = "volume"
    else:
        y_expr = "COUNT(*)::bigint"
        y_name = "linhas"

    q = f"""
        SELECT {mes_expr} AS mes, {y_expr} AS {y_name}
        FROM {tqname(SCHEMA, table_name)}
        WHERE {where_sql} AND {qname(col_hora)} IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    return sql_df(q, params)


@st.cache_data(show_spinner="🏆 Carregando TOPs (SQL)...")
def query_top_dim(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any], dim_key: str, topn: int = 10) -> pd.DataFrame:
    dim_col = cols.get(dim_key)
    if not dim_col:
        return pd.DataFrame()

    where_sql, params = build_where_sql(cols, filters, table_name)
    col_qtd = cols.get("qtd")

    if col_qtd:
        y_expr = f"COALESCE(SUM({numeric_expr(col_qtd)}),0)"
        y_name = "volume"
    else:
        y_expr = "COUNT(*)::bigint"
        y_name = "linhas"

    q = f"""
        SELECT {qname(dim_col)}::text AS dim, {y_expr} AS {y_name}
        FROM {tqname(SCHEMA, table_name)}
        WHERE {where_sql} AND {_non_empty_where(qname(dim_col))}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :lim
    """
    params2 = dict(params)
    params2["lim"] = int(topn)
    return sql_df(q, params2)


@st.cache_data(show_spinner="⚠️ Carregando risco (SQL)...")
def query_risk_dim(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any], dim_key: str, dias_crit: int) -> pd.DataFrame:
    dim_col = cols.get(dim_key)
    col_dias = cols.get("dias")
    col_qtd = cols.get("qtd")
    if not dim_col or not col_dias:
        return pd.DataFrame()

    where_sql, params = build_where_sql(cols, filters, table_name)
    dias_num = numeric_expr(col_dias)

    if col_qtd:
        qtd_expr = f"COALESCE(SUM({numeric_expr(col_qtd)}),0) AS qtd_pedidos"
        linhas_expr = "COUNT(*)::bigint AS linhas"
    else:
        qtd_expr = "COUNT(*)::bigint AS qtd_pedidos"
        linhas_expr = "COUNT(*)::bigint AS linhas"

    q = f"""
        SELECT
            {qname(dim_col)}::text AS dim,
            {qtd_expr},
            {linhas_expr},
            COALESCE(AVG({dias_num}),0) AS media_dias,
            COALESCE(MAX({dias_num}),0) AS max_dias,
            SUM(CASE WHEN {dias_num} >= :dias_crit THEN 1 ELSE 0 END)::bigint AS pedidos_crit
        FROM {tqname(SCHEMA, table_name)}
        WHERE {where_sql}
          AND {_non_empty_where(qname(dim_col))}
          AND {dias_num} IS NOT NULL
        GROUP BY 1
        ORDER BY 5 DESC
        LIMIT 5000
    """
    params2 = dict(params)
    params2["dias_crit"] = int(dias_crit)
    return sql_df(q, params2)


@st.cache_data(show_spinner="📄 Carregando detalhes (SQL)...")
def query_details_page(
    table_name: str,
    cols_sel: List[str],
    cols: Dict[str, Optional[str]],
    filters: Dict[str, Any],
    page: int,
    page_size: int,
) -> pd.DataFrame:
    where_sql, params = build_where_sql(cols, filters, table_name)
    all_cols = get_table_columns(table_name)

    safe_cols = [c for c in cols_sel if isinstance(c, str) and c in all_cols]
    if not safe_cols:
        safe_cols = all_cols

    select_list = ", ".join([qname(c) for c in safe_cols])
    offset = max(0, int(page)) * int(page_size)

    q = f"""
        SELECT {select_list}
        FROM {tqname(SCHEMA, table_name)}
        WHERE {where_sql}
        OFFSET :off
        LIMIT :lim
    """
    params2 = dict(params)
    params2["off"] = int(offset)
    params2["lim"] = int(page_size)
    return sql_df(q, params2)


# ==========================================================
# UI / RENDER
# ==========================================================
def render_header():
    st.markdown(
        """
        <div class="custom-card" style="margin-bottom: 24px;">
            <div style="display: flex; align-items: center; gap: 16px;">
                <div style="font-size: 2.5rem;">📦</div>
                <div>
                    <h1 style="margin: 0; color: #e2e8f0; font-weight: 700;">Painel de Análise - Sem Movimentação</h1>
                    <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
                        Modo responsivo: filtros e agregações no PostgreSQL
                    </p>
                </div>
            </div>
            <div style="margin-top: 16px; display: flex; gap: 12px; flex-wrap: wrap;">
                <span class="badge badge-primary">PostgreSQL</span>
                <span class="badge badge-success">Streamlit</span>
                <span class="badge badge-warning">SQL First</span>
                <span class="badge badge-info">Responsivo</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_filters_ui(table_name: str, cols: Dict[str, Optional[str]]) -> Dict[str, Any]:
    st.sidebar.markdown("## ⚙️ Filtros (SQL)")
    filters: Dict[str, Any] = {}

    col_dias = cols.get("dias")
    if col_dias:
        mn, mx = get_min_max_numeric(table_name, col_dias)
        if mx < mn:
            mn, mx = 0, 0
        if mx == mn:
            mx = mn + 1
        filters["dias_range"] = st.sidebar.slider(
            "📅 Dias sem movimentação",
            min_value=int(mn),
            max_value=int(mx),
            value=(int(mn), int(mx)),
            step=1,
        )

    col_hora = cols.get("hora_ult")
    if col_hora:
        meses = get_distinct_months(table_name, col_hora, limit=120)
        filters["meses"] = st.sidebar.multiselect("📆 Mês da última operação", meses, default=[]) if meses else []

    lim = CONFIG["PERF"]["DISTINCT_LIMIT"]

    def _ms(key: str, label: str) -> List[str]:
        col = cols.get(key)
        if not col:
            return []
        opts = get_distinct_values(table_name, col, limit=lim)
        return st.sidebar.multiselect(label, opts, default=[])

    filters["unid_resp"] = _ms("unid_resp", "🏢 Unidade responsável")
    filters["base_entrega"] = _ms("base_entrega", "🏠 Base de entrega")
    filters["est_dest"] = _ms("est_dest", "🗺️ Estado (UF) destino")
    filters["reg_resp"] = _ms("reg_resp", "🧭 Regional responsável")
    filters["aging"] = _ms("aging", "⏱️ Aging / Tipo de atraso")

    return filters


def render_kpis_sql(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any]):
    k = query_kpis(table_name, cols, filters)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(create_kpi_card("Pedidos (Linhas)", format_number(k["total_linhas"]), "", CONFIG["VISUAIS"]["info_color"]), unsafe_allow_html=True)
    with c2:
        st.markdown(create_kpi_card("Volume Total", format_number(k["total_volume"]), "", CONFIG["VISUAIS"]["success_color"]), unsafe_allow_html=True)
    with c3:
        st.markdown(create_kpi_card("Média Dias Sem Mov", f'{k["media_dias"]:.1f}', "dias", CONFIG["VISUAIS"]["warning_color"]), unsafe_allow_html=True)
    with c4:
        st.markdown(create_kpi_card("Máx. Dias Sem Mov", f'{k["max_dias"]:.0f}', "dias", CONFIG["VISUAIS"]["danger_color"]), unsafe_allow_html=True)

    st.markdown(
        f"""
        <div style="display: flex; gap: 16px; margin: 20px 0;">
            <div class="pill pill-green">Até {CONFIG["CRITICOS"]["dias_crit_1"]} dias = Ok</div>
            <div class="pill pill-yellow">≥ {CONFIG["CRITICOS"]["dias_crit_1"]} e &lt; {CONFIG["CRITICOS"]["dias_crit_2"]} dias = Atenção ({format_number(k["crit_1"])})</div>
            <div class="pill pill-red">≥ {CONFIG["CRITICOS"]["dias_crit_2"]} dias = Crítico ({format_number(k["crit_2"])})</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_charts_sql(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any]):
    col1, col2 = st.columns([2, 1.5])

    with col1:
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📊 Distribuição de Dias sem Movimentação (SQL)</div>', unsafe_allow_html=True)
        df_hist = query_hist_dias(table_name, cols, filters)
        if not df_hist.empty:
            fig = px.bar(df_hist, x="dias", y="linhas", template="plotly_dark", title="Contagem por dia (agrupado)")
            fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📝 Sem dados para histograma (ou coluna 'dias' não detectada).")
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📈 Volume por Mês (SQL)</div>', unsafe_allow_html=True)
        df_mes = query_volume_mes(table_name, cols, filters)
        if not df_mes.empty:
            ycol = "volume" if "volume" in df_mes.columns else "linhas"
            figm = px.bar(df_mes, x="mes", y=ycol, template="plotly_dark", title="Agrupado por mês")
            figm.update_layout(margin=dict(l=10, r=10, t=40, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(figm, use_container_width=True)
        else:
            st.info("📝 Mês indisponível (coluna de data não é timestamp/date ou não detectada).")
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🏢 Top 10 Unidades Responsáveis (SQL)</div>', unsafe_allow_html=True)
        df_top_u = query_top_dim(table_name, cols, filters, "unid_resp", topn=10)
        if not df_top_u.empty:
            ycol = "volume" if "volume" in df_top_u.columns else "linhas"
            fig = px.bar(df_top_u, x="dim", y=ycol, template="plotly_dark", title="Top 10")
            fig.update_layout(xaxis_title="", margin=dict(l=10, r=10, t=40, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📝 Coluna não encontrada ou sem dados.")
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🏠 Top 10 Bases de Entrega (SQL)</div>', unsafe_allow_html=True)
        df_top_b = query_top_dim(table_name, cols, filters, "base_entrega", topn=10)
        if not df_top_b.empty:
            ycol = "volume" if "volume" in df_top_b.columns else "linhas"
            fig = px.bar(df_top_b, x="dim", y=ycol, template="plotly_dark", title="Top 10")
            fig.update_layout(xaxis_title="", margin=dict(l=10, r=10, t=40, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📝 Coluna não encontrada ou sem dados.")
        st.markdown("</div>", unsafe_allow_html=True)


def render_risk_sql(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any], dim_key: str, title: str):
    st.markdown('<div class="custom-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{title} - Visão de Risco (SQL)</div>', unsafe_allow_html=True)

    dias_crit = st.slider(f"📊 Definir dias críticos ({title})", 2, 30, 10, 1, key=f"risk_slider_{dim_key}")
    df_risk = query_risk_dim(table_name, cols, filters, dim_key, dias_crit)

    if df_risk.empty:
        st.info("📝 Colunas necessárias não encontradas (dimensão ou dias).")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.dataframe(df_risk, use_container_width=True)

    top = df_risk.head(15).copy()
    fig = px.bar(top, x="dim", y="max_dias", color="max_dias", template="plotly_dark", title=f"Top {title} por Maior Tempo Parado")
    fig.update_layout(xaxis_title="", yaxis_title="Máx. Dias Sem Mov", margin=dict(l=10, r=10, t=40, b=10),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_detail_and_download(table_name: str, cols: Dict[str, Optional[str]], filters: Dict[str, Any]):
    st.markdown('<div class="custom-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📥 Tabela Detalhada (Paginada) & Download</div>', unsafe_allow_html=True)

    all_cols = get_table_columns(table_name)
    default_cols = [c for c in [
        cols.get("pedido"),
        cols.get("dias"),
        cols.get("unid_resp"),
        cols.get("reg_resp"),
        cols.get("reg_dest"),
        cols.get("est_dest"),
        cols.get("base_entrega"),
        cols.get("tipo_prod"),
        cols.get("cliente"),
        cols.get("nome_prob"),
    ] if c and c in all_cols]
    default_cols = list(dict.fromkeys(default_cols)) or all_cols

    cols_sel = st.multiselect("📋 Colunas para exibir", options=all_cols, default=default_cols)
    page_size = st.number_input("📄 Linhas por página", min_value=200, max_value=20000,
                                value=CONFIG["PERF"]["DETAIL_PAGE_SIZE"], step=200)
    page = st.number_input("➡️ Página (0 = primeira)", min_value=0, value=0, step=1)

    df_page = query_details_page(table_name, cols_sel, cols, filters, int(page), int(page_size))
    st.dataframe(df_page, use_container_width=True)

    st.markdown("---")
    max_export = st.number_input(
        "⬇️ Exportar até (linhas) — segurança",
        min_value=10_000,
        max_value=1_000_000,
        value=CONFIG["PERF"]["DETAIL_MAX_EXPORT"],
        step=10_000,
        help="Export muito grande pode ficar lento.",
    )

    if st.button("📄 Gerar CSV (SQL)"):
        where_sql, params = build_where_sql(cols, filters, table_name)
        safe_cols = [c for c in cols_sel if c in all_cols] or all_cols
        select_list = ", ".join([qname(c) for c in safe_cols])

        q = f"""
            SELECT {select_list}
            FROM {tqname(SCHEMA, table_name)}
            WHERE {where_sql}
            LIMIT :lim
        """
        params2 = dict(params)
        params2["lim"] = int(max_export)

        df_export = sql_df(q, params2)
        csv_bytes = df_export.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Baixar CSV", data=csv_bytes, file_name="sem_mov_filtrado.csv", mime="text/csv")

    st.markdown("</div>", unsafe_allow_html=True)


# ==========================================================
# MAIN
# ==========================================================
def main():
    render_header()

    tabelas = list_tables()
    if not tabelas:
        st.error("❌ Nenhuma tabela encontrada no schema public")
        return

    TABELA_SEM_MOV = "col_12_base_de_dados_sem_mov_novo"
    default_idx = tabelas.index(TABELA_SEM_MOV) if TABELA_SEM_MOV in tabelas else 0

    st.sidebar.markdown("## 🗃️ Fonte de Dados")
    tabela_escolhida = st.sidebar.selectbox("📋 Tabela (schema public)", options=tabelas, index=default_idx)

    carregar = st.sidebar.button("🚀 Carregar (Modo Responsivo)", type="primary")
    if not carregar:
        st.info("Escolha a tabela na esquerda e clique em **🚀 Carregar (Modo Responsivo)**.")
        return

    try:
        cols = detect_sem_mov_columns_db(tabela_escolhida)

        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🔍 Colunas Detectadas (DB)</div>', unsafe_allow_html=True)
        st.json({k: v for k, v in cols.items() if v})
        st.markdown("</div>", unsafe_allow_html=True)

        filters = render_filters_ui(tabela_escolhida, cols)

        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📈 KPIs Gerais (SQL)</div>', unsafe_allow_html=True)
        render_kpis_sql(tabela_escolhida, cols, filters)
        st.markdown("</div>", unsafe_allow_html=True)

        tab1, tab2, tab3, tab4 = st.tabs(
            ["📊 Visão Geral (SQL)", "🏢 Unidade Responsável (SQL)", "🏠 Base de Entrega (SQL)", "📥 Detalhes & Download"]
        )

        with tab1:
            render_charts_sql(tabela_escolhida, cols, filters)

        with tab2:
            render_risk_sql(tabela_escolhida, cols, filters, "unid_resp", "Unidade Responsável")

        with tab3:
            render_risk_sql(tabela_escolhida, cols, filters, "base_entrega", "Base de Entrega")

        with tab4:
            render_detail_and_download(tabela_escolhida, cols, filters)

    except Exception as e:
        st.error("❌ Erro inesperado")
        st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)))

if __name__ == "__main__":
    main()
