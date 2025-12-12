# app.py - Versão Completa
# -*- coding: utf-8 -*-

import pandas as pd
import plotly.express as px
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go  # (mantido se quiser usar depois)
from plotly.subplots import make_subplots  # (mantido se quiser usar depois)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect, text
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import altair as alt  # (mantido se quiser usar depois)

from db import get_engine

# ==========================================================
# CONFIGURAÇÕES GLOBAIS
# ==========================================================
CONFIG = {
    "PAGE_TITLE": "Sem Movimentação - PostgreSQL",
    "PAGE_ICON": "📦",
    "THEME": {
        "primaryColor": "#1f77b4",
        "backgroundColor": "#0e1117",
        "secondaryBackgroundColor": "#1e213a",
        "textColor": "#ffffff",
        "font": "sans serif"
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
        "aging": ["aging", "超时类型"]
    },
    "CRITICOS": {
        "dias_crit_1": 5,
        "dias_crit_2": 10,
        "dias_crit_3": 20
    },
    "VISUAIS": {
        "card_bg": "rgba(255,255,255,0.03)",
        "card_border": "rgba(255,255,255,0.12)",
        "success_color": "#10b981",
        "warning_color": "#f59e0b",
        "danger_color": "#ef4444",
        "info_color": "#3b82f6"
    }
}

# ==========================================================
# CONFIGURAÇÃO DO APP
# ==========================================================
st.set_page_config(
    page_title=CONFIG["PAGE_TITLE"],
    page_icon=CONFIG["PAGE_ICON"],
    layout="wide",
    initial_sidebar_state="expanded",
)

# Aplicar tema básico
st.markdown(f"""
    <style>
    :root {{
        --primary-color: {CONFIG["THEME"]["primaryColor"]};
        --background-color: {CONFIG["THEME"]["backgroundColor"]};
        --secondary-background: {CONFIG["THEME"]["secondaryBackgroundColor"]};
        --text-color: {CONFIG["THEME"]["textColor"]};
    }}
    .stApp {{
        background-color: var(--background-color);
        color: var(--text-color);
    }}
    .stSidebar {{
        background-color: var(--secondary-background);
    }}
    </style>
""", unsafe_allow_html=True)


# ==========================================================
# CSS AVANÇADO (VISUAL)
# ==========================================================
def inject_advanced_css():
    st.markdown("""
    <style>
    /* Layout e Container */
    .main {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        min-height: 100vh;
    }
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2.5rem;
        max-width: 1800px;
        background: transparent;
    }

    /* Cards Estilizados */
    .custom-card {
        background: var(--card-bg, rgba(30, 41, 59, 0.6));
        border: 1px solid var(--card-border, rgba(148, 163, 184, 0.2));
        border-radius: 16px;
        padding: 20px;
        margin: 12px 0;
        backdrop-filter: blur(10px);
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    .custom-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 15px rgba(0, 0, 0, 0.2);
    }

    /* Títulos */
    .section-title {
        font-size: 1.2rem;
        font-weight: 700;
        color: #e2e8f0;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(255, 255, 255, 0.1);
    }
    .section-sub {
        font-size: 0.95rem;
        color: #94a3b8;
        margin-bottom: 16px;
    }

    /* Badges e Pills */
    .badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 500;
        margin-right: 8px;
        margin-top: 4px;
        border: 1px solid;
        transition: all 0.2s;
    }
    .badge:hover {
        transform: scale(1.05);
    }
    .badge-primary { background: rgba(59, 130, 246, 0.2); border-color: #3b82f6; color: #dbeafe; }
    .badge-success { background: rgba(16, 185, 129, 0.2); border-color: #10b981; color: #d1fae5; }
    .badge-warning { background: rgba(245, 158, 11, 0.2); border-color: #f59e0b; color: #fed7aa; }
    .badge-danger { background: rgba(239, 68, 68, 0.2); border-color: #ef4444; color: #fecaca; }
    .badge-info { background: rgba(59, 130, 246, 0.2); border-color: #3b82f6; color: #dbeafe; }

    .pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 500;
        margin-right: 8px;
        margin-top: 4px;
        transition: all 0.2s;
    }
    .pill:hover { transform: scale(1.05); }
    .pill-green { background: rgba(16, 185, 129, 0.15); border: 1px solid rgba(16, 185, 129, 0.5); color: #10b981; }
    .pill-yellow { background: rgba(245, 158, 11, 0.15); border: 1px solid rgba(245, 158, 11, 0.5); color: #f59e0b; }
    .pill-red { background: rgba(239, 68, 68, 0.15); border: 1px solid rgba(239, 68, 68, 0.5); color: #ef4444; }
    .pill-blue { background: rgba(59, 130, 246, 0.15); border: 1px solid rgba(59, 130, 246, 0.5); color: #3b82f6; }

    /* KPIs */
    .kpi-container {
        background: rgba(30, 41, 59, 0.4);
        border-radius: 16px;
        padding: 16px;
        margin-bottom: 20px;
        border: 1px solid rgba(148, 163, 184, 0.2);
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #e2e8f0;
        margin-bottom: 4px;
    }
    .kpi-label {
        font-size: 0.9rem;
        color: #94a3b8;
        margin-bottom: 8px;
    }

    /* Tabelas */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }

    /* Gráficos */
    .plot-container {
        background: rgba(30, 41, 59, 0.4);
        border-radius: 12px;
        padding: 16px;
        margin: 12px 0;
        border: 1px solid rgba(148, 163, 184, 0.2);
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        padding: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background: rgba(30, 41, 59, 0.6);
        border-radius: 8px;
        padding: 12px;
        font-weight: 500;
        transition: all 0.3s;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(59, 130, 246, 0.2);
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: rgba(59, 130, 246, 0.3);
        border-bottom: 2px solid #3b82f6;
    }

    /* Botões */
    .stButton>button {
        background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: 600;
        transition: all 0.3s;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
        background: linear-gradient(135deg, #2563eb 0%, #1e40af 100%);
    }

    /* Progress Bars */
    .progress-container {
        margin: 16px 0;
    }
    .progress-bar {
        height: 8px;
        border-radius: 4px;
        background: rgba(148, 163, 184, 0.2);
        overflow: hidden;
    }
    .progress-fill {
        height: 100%;
        border-radius: 4px;
        transition: width 0.5s ease;
    }

    /* Animações */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .fade-in {
        animation: fadeIn 0.5s ease-out;
    }

    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: rgba(30, 41, 59, 0.3);
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(59, 130, 246, 0.5);
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(59, 130, 246, 0.7);
    }
    </style>
    """, unsafe_allow_html=True)


inject_advanced_css()


# ==========================================================
# UTILITÁRIOS
# ==========================================================
def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços em branco dos nomes das colunas."""
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    return df


def format_number(num: float, decimals: int = 0) -> str:
    """Formata números com separador de milhar."""
    try:
        if pd.isna(num):
            return "0"
        num = float(num)
        if decimals == 0:
            return f"{int(num):,}".replace(",", ".")
        return f"{num:,.{decimals}f}".replace(",", ".")
    except (ValueError, TypeError):
        return "0"


def format_percentage(value: float, decimals: int = 2) -> str:
    """Formata porcentagem."""
    if pd.isna(value):
        return "0%"
    return f"{value * 100:.{decimals}f}%"


def _sum_numeric(df: pd.DataFrame, col: Optional[str]) -> float:
    """
    Soma segura de uma coluna numérica.
    Se a coluna não existir, retorna 0.0.
    """
    if not col or col not in df.columns:
        return 0.0
    return float(
        pd.to_numeric(df[col], errors="coerce")
        .fillna(0)
        .sum()
    )


def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "dados") -> bytes:
    """
    Converte um DataFrame em bytes de Excel para download.
    """
    from io import BytesIO

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return output.getvalue()


def create_progress_bar(value: float, max_value: float, color: str = "#3b82f6") -> str:
    """Cria uma barra de progresso visual."""
    percentage = (value / max_value) * 100 if max_value > 0 else 0
    return f"""
    <div class="progress-container">
        <div class="progress-bar">
            <div class="progress-fill" style="width: {percentage}%; background-color: {color};"></div>
        </div>
        <div style="text-align: center; margin-top: 4px; font-size: 0.85rem; color: #94a3b8;">
            {percentage:.1f}%
        </div>
    </div>
    """


def create_kpi_card(title: str, value: str, subtitle: str = "", color: str = "#3b82f6") -> str:
    """Cria um card de KPI estilizado."""
    return f"""
    <div class="kpi-container fade-in">
        <div class="kpi-label">{title}</div>
        <div class="kpi-value" style="color: {color};">{value}</div>
        {f'<div class="kpi-subtitle" style="color: #94a3b8; font-size: 0.85rem;">{subtitle}</div>' if subtitle else ''}
    </div>
    """


def detect_columns(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    """Detecta colunas baseado em padrões de similaridade."""
    colunas_lower = {c.lower(): c for c in df.columns if isinstance(c, str)}
    for pattern in patterns:
        pattern_low = pattern.lower()
        for nome_lower, nome_original in colunas_lower.items():
            if pattern_low in nome_lower:
                return nome_original
    return None


def detect_sem_mov_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """Detecta colunas específicas para a tabela Sem Movimentação."""
    return {
        key: detect_columns(df, patterns)
        for key, patterns in CONFIG["COLUNAS_SEM_MOV"].items()
    }


def prepare_datetime_columns(df: pd.DataFrame, cols: Dict[str, Optional[str]]) -> pd.DataFrame:
    """Prepara colunas de data e hora."""
    col_hora = cols.get("hora_ult")
    if col_hora and col_hora in df.columns:
        try:
            df[col_hora] = pd.to_datetime(
                df[col_hora].astype(str).str.strip().str.replace(r"\s+", " ", regex=True),
                format="%d/%m/%Y %H:%M:%S",
                errors="coerce"
            )
            df["__mes_ult_op"] = df[col_hora].dt.to_period("M").astype(str)
        except Exception as e:
            st.warning(f"Erro ao processar coluna de data: {e}")
    return df


# ==========================================================
# FILTROS
# ==========================================================
def apply_filters(df: pd.DataFrame, cols: Dict[str, Optional[str]]) -> pd.DataFrame:
    """Aplica filtros interativos (sidebar)."""
    df_filtered = df.copy()

    # Filtro por dias sem movimentação
    col_dias = cols.get("dias")
    if col_dias and col_dias in df_filtered.columns:
        df_filtered[col_dias] = pd.to_numeric(df_filtered[col_dias], errors="coerce")
        if df_filtered[col_dias].notna().any():
            min_val = int(df_filtered[col_dias].min())
            max_val = int(df_filtered[col_dias].max())
        else:
            min_val, max_val = 0, 1

        dias_range = st.sidebar.slider(
            "📅 Dias sem movimentação",
            min_value=min_val,
            max_value=max_val,
            value=(min_val, max_val),
            step=1,
            help="Selecione o intervalo de dias sem movimentação",
        )
        df_filtered = df_filtered[
            (df_filtered[col_dias] >= dias_range[0]) &
            (df_filtered[col_dias] <= dias_range[1])
        ]

    # Filtro por mês da última operação
    if "__mes_ult_op" in df_filtered.columns:
        meses = sorted(df_filtered["__mes_ult_op"].dropna().unique().tolist())
        selected_meses = st.sidebar.multiselect(
            "📆 Mês da última operação",
            meses,
            default=[],
            help="Selecione os meses para filtrar",
        )
        if selected_meses:
            df_filtered = df_filtered[df_filtered["__mes_ult_op"].isin(selected_meses)]

    # Filtros por dimensões principais
    for col_key, col_name in [
        ("unid_resp", "Unidade responsável"),
        ("base_entrega", "Base de entrega"),
        ("est_dest", "Estado de Destino (UF)"),
        ("reg_resp", "Regional responsável"),
        ("aging", "Aging / Tipo de atraso"),
    ]:
        col = cols.get(col_key)
        if col and col in df_filtered.columns:
            valores = sorted(
                [v for v in df_filtered[col].dropna().unique() if str(v).strip()]
            )
            selected = st.sidebar.multiselect(
                f"🏢 {col_name}",
                valores,
                default=[],
                help=f"Filtre por {col_name.lower()}",
            )
            if selected:
                df_filtered = df_filtered[df_filtered[col].isin(selected)]

    return df_filtered


# ==========================================================
# KPIs
# ==========================================================
def create_kpi_dashboard(df: pd.DataFrame, cols: Dict[str, Optional[str]]) -> None:
    """Cria dashboard de KPIs visualmente atraente."""
    col_dias = cols.get("dias")
    col_qtd = cols.get("qtd")

    if col_dias and col_dias in df.columns:
        df[col_dias] = pd.to_numeric(df[col_dias], errors="coerce")

    total_linhas = len(df)
    total_pedidos = _sum_numeric(df, col_qtd) if col_qtd and col_qtd in df.columns else total_linhas
    media_dias = (
        float(df[col_dias].mean(skipna=True))
        if col_dias and col_dias in df.columns and df[col_dias].notna().any()
        else 0
    )
    max_dias = (
        float(df[col_dias].max(skipna=True))
        if col_dias and col_dias in df.columns and df[col_dias].notna().any()
        else 0
    )

    crit_1 = int((df[col_dias] >= CONFIG["CRITICOS"]["dias_crit_1"]).sum()) if col_dias in df.columns else 0
    crit_2 = int((df[col_dias] >= CONFIG["CRITICOS"]["dias_crit_2"]).sum()) if col_dias in df.columns else 0
    crit_3 = int((df[col_dias] >= CONFIG["CRITICOS"]["dias_crit_3"]).sum()) if col_dias in df.columns else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(
            create_kpi_card(
                "Pedidos (Linhas)",
                format_number(total_linhas),
                "",
                CONFIG["VISUAIS"]["info_color"],
            ),
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            create_kpi_card(
                "Volume Total",
                format_number(total_pedidos),
                "",
                CONFIG["VISUAIS"]["success_color"],
            ),
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            create_kpi_card(
                "Média Dias Sem Mov",
                f"{media_dias:.1f}",
                "dias",
                CONFIG["VISUAIS"]["warning_color"],
            ),
            unsafe_allow_html=True,
        )

    with col4:
        st.markdown(
            create_kpi_card(
                "Máx. Dias Sem Mov",
                f"{max_dias:.0f}",
                "dias",
                CONFIG["VISUAIS"]["danger_color"],
            ),
            unsafe_allow_html=True,
        )

    # Indicadores de risco (legenda)
    st.markdown(
        """
        <div style="display: flex; gap: 16px; margin: 20px 0;">
            <div class="pill pill-green">Até {0} dias = Ok</div>
            <div class="pill pill-yellow">≥ {0} e &lt; {1} dias = Atenção ({2})</div>
            <div class="pill pill-red">≥ {1} dias = Crítico ({3})</div>
        </div>
        """.format(
            CONFIG["CRITICOS"]["dias_crit_1"],
            CONFIG["CRITICOS"]["dias_crit_2"],
            format_number(crit_1),
            format_number(crit_2),
        ),
        unsafe_allow_html=True,
    )

    col5, col6, col7 = st.columns(3)

    with col5:
        st.markdown(
            create_kpi_card(
                f"Pedidos ≥ {CONFIG['CRITICOS']['dias_crit_1']} dias",
                format_number(crit_1),
                "",
                CONFIG["VISUAIS"]["warning_color"],
            ),
            unsafe_allow_html=True,
        )

    with col6:
        st.markdown(
            create_kpi_card(
                f"Pedidos ≥ {CONFIG['CRITICOS']['dias_crit_2']} dias",
                format_number(crit_2),
                "",
                CONFIG["VISUAIS"]["danger_color"],
            ),
            unsafe_allow_html=True,
        )

    with col7:
        st.markdown(
            create_kpi_card(
                f"Pedidos ≥ {CONFIG['CRITICOS']['dias_crit_3']} dias",
                format_number(crit_3),
                "",
                CONFIG["VISUAIS"]["danger_color"],
            ),
            unsafe_allow_html=True,
        )


# ==========================================================
# GRÁFICOS PRINCIPAIS
# ==========================================================
def create_visual_charts(df: pd.DataFrame, cols: Dict[str, Optional[str]]) -> None:
    """Cria gráficos visuais avançados."""
    col_dias = cols.get("dias")
    col_unid = cols.get("unid_resp")
    col_base = cols.get("base_entrega")
    col_qtd = cols.get("qtd")

    col1, col2 = st.columns([2, 1.5])

    # Histograma + volume por mês
    with col1:
        st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📊 Distribuição de Dias sem Movimentação</div>', unsafe_allow_html=True)

        if col_dias and col_dias in df.columns:
            fig = px.histogram(
                df,
                x=col_dias,
                nbins=30,
                title="Histograma de dias sem movimentação",
                color_discrete_sequence=['#3b82f6'],
                template='plotly_dark',
            )
            fig.update_layout(
                margin=dict(l=10, r=10, t=40, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📝 Coluna de Dias sem Mov não encontrada.")

        st.markdown('</div>', unsafe_allow_html=True)

        # Gráfico de barras por mês
        st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📈 Volume por Mês</div>', unsafe_allow_html=True)

        if "__mes_ult_op" in df.columns:
            df_mes = df.copy()
            if col_qtd and col_qtd in df_mes.columns:
                df_mes[col_qtd] = pd.to_numeric(df_mes[col_qtd], errors="coerce").fillna(0)
                gp_mes = df_mes.groupby("__mes_ult_op")[col_qtd].sum().reset_index()
                y_col = col_qtd
            else:
                gp_mes = df_mes.groupby("__mes_ult_op").size().reset_index(name="qtd")
                y_col = "qtd"

            fig_m = px.bar(
                gp_mes,
                x="__mes_ult_op",
                y=y_col,
                title="Pedidos por mês da última operação",
                color_discrete_sequence=['#10b981'],
                template='plotly_dark',
            )
            fig_m.update_layout(
                xaxis_title="Mês (YYYY-MM)",
                yaxis_title="Quantidade",
                margin=dict(l=10, r=10, t=40, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
            )
            st.plotly_chart(fig_m, use_container_width=True)
        else:
            st.info("📝 Coluna de horário da última operação não pôde ser convertida.")

        st.markdown('</div>', unsafe_allow_html=True)

    # Top unidades / bases
    with col2:
        # Top 10 Unidades Responsáveis
        st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🏢 Top 10 Unidades Responsáveis</div>', unsafe_allow_html=True)

        if col_unid and col_unid in df.columns:
            df_top = df.copy()
            if col_qtd and col_qtd in df_top.columns:
                df_top[col_qtd] = pd.to_numeric(df_top[col_qtd], errors="coerce").fillna(0)
                gp = df_top.groupby(col_unid)[col_qtd].sum().reset_index()
                gp = gp.sort_values(col_qtd, ascending=False).head(10)
                fig2 = px.bar(
                    gp,
                    x=col_unid,
                    y=col_qtd,
                    title="Top 10 unidades (Pedidos件量)",
                    color_discrete_sequence=['#f59e0b'],
                    template='plotly_dark',
                )
            else:
                gp = df_top.groupby(col_unid).size().reset_index(name="qtd")
                gp = gp.sort_values("qtd", ascending=False).head(10)
                fig2 = px.bar(
                    gp,
                    x=col_unid,
                    y="qtd",
                    title="Top 10 unidades (linhas)",
                    color_discrete_sequence=['#f59e0b'],
                    template='plotly_dark',
                )

            fig2.update_layout(
                xaxis_title="",
                yaxis_title="Quantidade",
                margin=dict(l=10, r=10, t=40, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("📝 Coluna de Unidade responsável não encontrada.")
        st.markdown('</div>', unsafe_allow_html=True)

        # Top 10 Bases de Entrega
        st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🏠 Top 10 Bases de Entrega</div>', unsafe_allow_html=True)

        if col_base and col_base in df.columns:
            df_topb = df.copy()
            if col_qtd and col_qtd in df_topb.columns:
                df_topb[col_qtd] = pd.to_numeric(df_topb[col_qtd], errors="coerce").fillna(0)
                gp = df_topb.groupby(col_base)[col_qtd].sum().reset_index()
                gp = gp.sort_values(col_qtd, ascending=False).head(10)
                fig3 = px.bar(
                    gp,
                    x=col_base,
                    y=col_qtd,
                    title="Top 10 bases (Pedidos件量)",
                    color_discrete_sequence=['#ef4444'],
                    template='plotly_dark',
                )
            else:
                gp = df_topb.groupby(col_base).size().reset_index(name="qtd")
                gp = gp.sort_values("qtd", ascending=False).head(10)
                fig3 = px.bar(
                    gp,
                    x=col_base,
                    y="qtd",
                    title="Top 10 bases (linhas)",
                    color_discrete_sequence=['#ef4444'],
                    template='plotly_dark',
                )

            fig3.update_layout(
                xaxis_title="",
                yaxis_title="Quantidade",
                margin=dict(l=10, r=10, t=40, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("📝 Coluna de Base de entrega não encontrada.")
        st.markdown('</div>', unsafe_allow_html=True)


# ==========================================================
# ANÁLISE DE RISCO
# ==========================================================
def create_risk_analysis(df: pd.DataFrame, cols: Dict[str, Optional[str]], dimension: str, title: str) -> None:
    """Cria análise de risco visual por dimensão (unidade/base/etc)."""
    dim_col = cols.get(dimension)
    if not dim_col or dim_col not in df.columns:
        st.info(f"📝 Coluna de {title} não encontrada.")
        return

    dias_crit = st.slider(
        f"📊 Definir dias críticos ({title})",
        2, 30, 10, 1,
        help="Ajuste o limite para considerar como risco",
    )

    col_qtd = cols.get("qtd")
    col_dias = cols.get("dias")

    df_group = df.copy()
    if col_dias and col_dias in df_group.columns:
        df_group[col_dias] = pd.to_numeric(df_group[col_dias], errors="coerce")

    if col_qtd and col_qtd in df_group.columns:
        df_group[col_qtd] = pd.to_numeric(df_group[col_qtd], errors="coerce").fillna(0)
        agg = df_group.groupby(dim_col).agg(
            qtd_pedidos=(col_qtd, "sum"),
            linhas=(dim_col, "count"),
            media_dias=(col_dias, "mean"),
            max_dias=(col_dias, "max"),
            pedidos_crit=(col_dias, lambda x: (x >= dias_crit).sum()),
        )
    else:
        agg = df_group.groupby(dim_col).agg(
            qtd_pedidos=(dim_col, "count"),
            media_dias=(col_dias, "mean"),
            max_dias=(col_dias, "max"),
            pedidos_crit=(col_dias, lambda x: (x >= dias_crit).sum()),
        )

    agg = agg.reset_index()
    agg["media_dias"] = agg["media_dias"].fillna(0)
    agg["max_dias"] = agg["max_dias"].fillna(0)
    agg = agg.sort_values("max_dias", ascending=False)

    # Tabela
    st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{title} - Visão de Risco</div>', unsafe_allow_html=True)
    st.dataframe(agg, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Gráfico
    top_risco = agg.head(15)
    fig = px.bar(
        top_risco,
        x=dim_col,
        y="max_dias",
        title=f"Top {title} por Maior Tempo Parado",
        color="max_dias",
        color_continuous_scale=["#10b981", "#f59e0b", "#ef4444"],
        template='plotly_dark',
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Máx. Dias Sem Mov",
        margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#e2e8f0'),
    )
    st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# DOWNLOAD / TABELA DETALHADA
# ==========================================================
def create_download_section(df: pd.DataFrame, cols: Dict[str, Optional[str]]) -> None:
    """Cria seção de download com visualização melhorada."""
    st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📥 Download de Dados</div>', unsafe_allow_html=True)

    all_cols = df.columns.tolist()
    default_cols: List[str] = []
    for key in [
        "pedido",
        "dias",
        "unid_resp",
        "reg_resp",
        "reg_dest",
        "est_dest",
        "base_entrega",
        "tipo_prod",
        "cliente",
        "nome_prob",
    ]:
        c = cols.get(key)
        if c and c in all_cols:
            default_cols.append(c)
    default_cols = list(dict.fromkeys(default_cols))

    cols_sel = st.multiselect(
        "📋 Colunas para exibir",
        options=all_cols,
        default=default_cols if default_cols else all_cols,
        help="Selecione as colunas que deseja incluir no download",
    )

    df_show = df[cols_sel] if cols_sel else df
    st.dataframe(df_show, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        excel_bytes = df_to_excel_bytes(df_show, sheet_name="sem_mov")
        st.download_button(
            label="📄 Baixar Excel",
            data=excel_bytes,
            file_name="sem_movimentacao_filtrado.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Baixe os dados filtrados em formato Excel",
        )

    with col2:
        csv_bytes = df_show.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📄 Baixar CSV",
            data=csv_bytes,
            file_name="sem_movimentacao_filtrado.csv",
            mime="text/csv",
            help="Baixe os dados filtrados em formato CSV",
        )

    st.markdown('</div>', unsafe_allow_html=True)


# ==========================================================
# CONEXÃO COM BANCO
# ==========================================================
@st.cache_resource
def get_db_engine():
    return get_engine()


@st.cache_data(show_spinner="🔍 Carregando tabelas...")
def list_tables() -> List[str]:
    try:
        engine = get_db_engine()
        insp = inspect(engine)
        return sorted(insp.get_table_names(schema="public"))
    except Exception as e:
        st.error("❌ Erro ao listar tabelas")
        st.code(repr(e))
        return []


@st.cache_data(show_spinner="📊 Carregando dados...")
def load_table(table_name: str, limit: int = 300_000) -> pd.DataFrame:
    try:
        engine = get_db_engine()
        query = text(f'SELECT * FROM public."{table_name}" LIMIT :limite')
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"limite": limit})
        return _strip_columns(df)
    except Exception as e:
        st.error(f"❌ Erro ao carregar tabela {table_name}")
        st.code(repr(e))
        return pd.DataFrame()


# ==========================================================
# HEADER
# ==========================================================
def render_header():
    """Renderiza o cabeçalho com visual melhorado."""
    st.markdown(
        """
        <div class="custom-card fade-in" style="margin-bottom: 24px;">
            <div style="display: flex; align-items: center; gap: 16px;">
                <div style="font-size: 2.5rem;">📦</div>
                <div>
                    <h1 style="margin: 0; color: #e2e8f0; font-weight: 700;">Painel de Análise - Sem Movimentação</h1>
                    <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
                        Visualização avançada de dados do PostgreSQL com análise operacional
                    </p>
                </div>
            </div>
            <div style="margin-top: 16px; display: flex; gap: 12px; flex-wrap: wrap;">
                <span class="badge badge-primary">PostgreSQL</span>
                <span class="badge badge-success">Streamlit</span>
                <span class="badge badge-warning">Análise de Risco</span>
                <span class="badge badge-info">Dashboard Interativo</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ==========================================================
# MAIN
# ==========================================================
def main():
    render_header()

    st.sidebar.markdown("## ⚙️ Configurações")

    try:
        tabelas = list_tables()
    except Exception as e:
        st.error("❌ Erro ao listar tabelas")
        st.code(repr(e))
        return

    if not tabelas:
        st.error("❌ Nenhuma tabela encontrada no schema public")
        return

    TABELA_SEM_MOV = "col_12_base_de_dados_sem_mov_novo"
    default_idx = tabelas.index(TABELA_SEM_MOV) if TABELA_SEM_MOV in tabelas else 0

    tabela_escolhida = st.sidebar.selectbox(
        "📋 Tabela (schema public)",
        options=tabelas,
        index=default_idx,
        help="Selecione a tabela para análise",
    )

    limite = st.sidebar.number_input(
        "📏 Limite de linhas",
        min_value=10_000,
        max_value=1_000_000,
        value=300_000,
        step=10_000,
        help="Número máximo de linhas a carregar",
    )

    carregar = st.sidebar.button(
        "🚀 Carregar Dados",
        type="primary",
        help="Inicia o carregamento dos dados",
    )

    if not carregar:
        st.info("Configure o limite / tabela na esquerda e clique em **🚀 Carregar Dados**.")
        return

    with st.spinner("⏳ Carregando dados... Aguarde"):
        try:
            df_raw = load_table(tabela_escolhida, limite)

            if df_raw.empty:
                st.warning(f"⚠️ Nenhuma linha retornada da tabela `{tabela_escolhida}`")
                return

            # Caso 1 – Tabela Sem Mov
            if tabela_escolhida == TABELA_SEM_MOV:
                cols = detect_sem_mov_columns(df_raw)
                df_raw = prepare_datetime_columns(df_raw, cols)

                # Card de colunas detectadas (apenas as não-nulas)
                st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">🔍 Colunas Detectadas</div>', unsafe_allow_html=True)
                st.json({k: v for k, v in cols.items() if v})
                st.markdown('</div>', unsafe_allow_html=True)

                # Filtros
                df_filtered = apply_filters(df_raw, cols)

                # Botão limpar filtros
                if st.button("🔄 Limpar Filtros"):
                    st.experimental_rerun()

                # KPIs
                st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">📈 KPIs Gerais</div>', unsafe_allow_html=True)
                create_kpi_dashboard(df_filtered, cols)
                st.markdown('</div>', unsafe_allow_html=True)

                # Tabs
                tab1, tab2, tab3, tab4 = st.tabs(
                    [
                        "📊 Visão Geral",
                        "🏢 Unidade Responsável",
                        "🏠 Base de Entrega",
                        "📥 Tabela & Download",
                    ]
                )

                with tab1:
                    create_visual_charts(df_filtered, cols)

                with tab2:
                    create_risk_analysis(df_filtered, cols, "unid_resp", "Unidade Responsável")

                with tab3:
                    create_risk_analysis(df_filtered, cols, "base_entrega", "Base de Entrega")

                with tab4:
                    create_download_section(df_filtered, cols)

            # Caso 2 – Outras tabelas (visão genérica)
            else:
                st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
                st.markdown(
                    f'<div class="section-title">📋 Tabela: {tabela_escolhida}</div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    '<div class="section-sub">Visualização genérica de dados</div>',
                    unsafe_allow_html=True,
                )
                st.markdown('</div>', unsafe_allow_html=True)

                st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
                st.markdown('<div class="section-title">📊 Resumo da Tabela</div>', unsafe_allow_html=True)
                col1, col2, col3 = st.columns(3)
                col1.metric("Linhas", format_number(len(df_raw)))
                col2.metric("Colunas", format_number(df_raw.shape[1]))
                col3.metric(
                    "Numéricas",
                    format_number(len(df_raw.select_dtypes(include=["number"]).columns)),
                )
                st.markdown('</div>', unsafe_allow_html=True)

                tab1, tab2 = st.tabs(["📋 Dados & Estatísticas", "📥 Download"])

                with tab1:
                    st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">📈 Estatísticas</div>', unsafe_allow_html=True)
                    st.dataframe(df_raw.describe(include="all").T, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                    st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">📋 Visualização</div>', unsafe_allow_html=True)
                    st.dataframe(df_raw, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                with tab2:
                    st.markdown('<div class="custom-card fade-in">', unsafe_allow_html=True)
                    st.markdown('<div class="section-title">📥 Download</div>', unsafe_allow_html=True)

                    excel_bytes = df_to_excel_bytes(df_raw, sheet_name=tabela_escolhida[:31])
                    st.download_button(
                        label="📄 Baixar Excel",
                        data=excel_bytes,
                        file_name=f"{tabela_escolhida}_dados.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                    st.markdown('</div>', unsafe_allow_html=True)

        except Exception as e:
            st.error("❌ Erro inesperado")
            st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)))


if __name__ == "__main__":
    main()
