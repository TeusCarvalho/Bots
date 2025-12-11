# -*- coding: utf-8 -*-
# app_sem_mov.py
# pip install streamlit pandas numpy plotly openpyxl

import os
from pathlib import Path
from typing import List, Tuple, Optional

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px


# ==========================================================
# CONFIG
# ==========================================================
st.set_page_config(
    page_title="Pedidos sem Movimentação",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================================
# MAPEAMENTO DE COLUNAS DO SEU MODELO (PT+CN -> PT CANÔNICO)
# ==========================================================
COL_MAP = {
    "Número de pedido JMS 运单号": "Numero pedido JMS",
    "Pedidos件量": "Qtd pedidos",
    "Regional Remetente寄件代理区": "Regional Remetente",
    "Código da Base Remetente寄件网点编码": "Codigo Base Remetente",
    "Nome da Base Remetente寄件网点名称": "Nome Base Remetente",
    "Regional mais recente最新操作代理区": "Regional Ultima Operacao",
    "Código da base mais recente最新操作机构编码": "Codigo Base Ultima Operacao",
    "Nome da base mais recente最新操作机构名称": "Nome Base Ultima Operacao",
    "Tipo da última operação最新操作类型": "Tipo Ultima Operacao",
    "Operador do bipe mais recente最新操作人": "Operador Ultima Operacao",
    "Horário da última operação最新操作时间": "Horario Ultima Operacao",
    "Próxima parada发件下一站": "Proxima Parada",
    "Aging超时类型": "Aging",
    "Nome cliente客户简称": "Nome Cliente",
    "Tipo de produto产品类型": "Tipo Produto",
    "Origem do Pedido订单来源": "Origem Pedido",
    "Número do ID任务单号": "Numero ID",
    "Nome de pacote problemático问题件名称": "Nome Pacote Problematico",
    "Unidade responsável责任机构": "Unidade Responsavel",
    "Regional responsável责任所属代理区": "Regional Responsavel",
    "Regional Destino目的代理区": "Regional Destino",
    "Estado de Destino目的州": "UF Destino",
    "Base de entrega派件网点": "Base Entrega",
}

# Colunas canônicas mais importantes
COL_PEDIDO = "Numero pedido JMS"
COL_QTD = "Qtd pedidos"
COL_TIPO_ULT = "Tipo Ultima Operacao"
COL_HORA_ULT = "Horario Ultima Operacao"
COL_PROB = "Nome Pacote Problematico"
COL_UNIDADE = "Unidade Responsavel"
COL_BASE_ENT = "Base Entrega"
COL_REG_RESP = "Regional Responsavel"
COL_TIPO_PROD = "Tipo Produto"
COL_ORIGEM = "Origem Pedido"
COL_UF = "UF Destino"

COL_DIAS_SEM_MOV = "Dias sem movimentacao"


# ==========================================================
# CSS LEVE PARA FICAR MAIS VISUAL
# ==========================================================
def inject_css():
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
        [data-testid="stSidebar"] { padding-top: 1rem; }

        .section-card {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
            padding: 14px 16px;
            margin: 6px 0 14px 0;
        }

        [data-testid="stMetricValue"] { font-size: 1.35rem; }
        </style>
        """,
        unsafe_allow_html=True
    )

inject_css()


# ==========================================================
# UTILITÁRIOS DE ARQUIVO
# ==========================================================
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
# NORMALIZAÇÃO DO MODELO
# ==========================================================
def normalize_model(df: pd.DataFrame, arquivo_origem: str = "") -> pd.DataFrame:
    # Renomeia somente as colunas presentes
    rename_map = {c: COL_MAP[c] for c in df.columns if c in COL_MAP}
    if rename_map:
        df = df.rename(columns=rename_map)

    # Converte datetime
    if COL_HORA_ULT in df.columns:
        df[COL_HORA_ULT] = pd.to_datetime(df[COL_HORA_ULT], errors="coerce")

    # Numéricos
    if COL_QTD in df.columns:
        df[COL_QTD] = pd.to_numeric(df[COL_QTD], errors="coerce").fillna(0).astype("int64")

    # Metadata
    if arquivo_origem:
        df["__arquivo_origem"] = arquivo_origem

    # Dias sem movimentação
    if COL_HORA_ULT in df.columns:
        hoje = pd.Timestamp.now().normalize()
        df[COL_DIAS_SEM_MOV] = (hoje - df[COL_HORA_ULT].dt.normalize()).dt.days
    else:
        df[COL_DIAS_SEM_MOV] = np.nan

    return df


# ==========================================================
# LEITURA COM CACHE
# ==========================================================
@st.cache_data(show_spinner=False)
def load_excel_file(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)
    df = normalize_model(df, arquivo_origem=Path(path).name)
    return df


@st.cache_data(show_spinner=False)
def load_from_folder_cached(folder: str, signature: Tuple[Tuple[str, float], ...]) -> pd.DataFrame:
    _ = signature

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
    _ = file_sizes

    dfs = []
    from io import BytesIO

    for name, b in zip(file_names, files_bytes):
        try:
            df = pd.read_excel(BytesIO(b), sheet_name=0)
            df = normalize_model(df, arquivo_origem=name)
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
    st.sidebar.markdown("### Filtros")

    # Período baseado na última operação
    if COL_HORA_ULT in df.columns:
        min_d = df[COL_HORA_ULT].min()
        max_d = df[COL_HORA_ULT].max()

        with st.sidebar.expander("Período da última operação", expanded=True):
            if not pd.isna(min_d) and not pd.isna(max_d):
                period = st.date_input(
                    "Intervalo",
                    value=(min_d.date(), max_d.date()),
                    min_value=min_d.date(),
                    max_value=max_d.date()
                )
                if isinstance(period, tuple) and len(period) == 2:
                    d1, d2 = period
                    df = df[df[COL_HORA_ULT].between(pd.to_datetime(d1), pd.to_datetime(d2) + pd.Timedelta(days=1))]

    # Filtro por dias sem movimentação
    with st.sidebar.expander("Aging (dias sem movimentação)", expanded=True):
        if COL_DIAS_SEM_MOV in df.columns:
            max_days = int(np.nanmax(df[COL_DIAS_SEM_MOV].values)) if len(df) else 0
            max_days = max(max_days, 1)
            min_days_sel = st.slider("Mostrar a partir de (dias)", 0, max_days, 0, step=1)
            df = df[df[COL_DIAS_SEM_MOV] >= min_days_sel]

    # Dimensões
    dim_cols = [
        COL_UNIDADE,
        COL_BASE_ENT,
        COL_REG_RESP,
        COL_TIPO_ULT,
        COL_PROB,
        COL_TIPO_PROD,
        COL_ORIGEM,
        COL_UF,
    ]

    with st.sidebar.expander("Dimensões", expanded=True):
        for col in dim_cols:
            if col in df.columns:
                opts = sorted([x for x in df[col].dropna().unique().tolist() if str(x).strip() != ""])
                if opts:
                    sel = st.multiselect(col, opts, default=[])
                    if sel:
                        df = df[df[col].isin(sel)]

    return df


# ==========================================================
# KPIs
# ==========================================================
def build_kpis(df: pd.DataFrame):
    total_linhas = len(df)

    # pedidos únicos
    if COL_PEDIDO in df.columns:
        total_pedidos_unicos = df[COL_PEDIDO].nunique(dropna=True)
    else:
        total_pedidos_unicos = 0

    # soma de quantidade (quando existir)
    total_qtd = int(df[COL_QTD].sum()) if COL_QTD in df.columns else total_linhas

    # problema
    if COL_PROB in df.columns:
        com_problema = int(df[COL_PROB].notna().sum())
        taxa_prob = com_problema / total_linhas if total_linhas else 0
    else:
        com_problema = 0
        taxa_prob = 0

    # dias sem mov
    if COL_DIAS_SEM_MOV in df.columns:
        mediana_dias = float(np.nanmedian(df[COL_DIAS_SEM_MOV].values)) if total_linhas else 0
        media_dias = float(np.nanmean(df[COL_DIAS_SEM_MOV].values)) if total_linhas else 0
    else:
        mediana_dias = 0
        media_dias = 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    c1.metric("Pedidos únicos", f"{total_pedidos_unicos:,}".replace(",", "."))
    c2.metric("Qtd total (coluna Pedidos)", f"{total_qtd:,}".replace(",", "."))
    c3.metric("Linhas", f"{total_linhas:,}".replace(",", "."))
    c4.metric("Com pacote problemático", f"{com_problema:,}".replace(",", "."))
    c5.metric("Taxa de problema", f"{taxa_prob*100:.2f}%")
    c6.metric("Mediana dias sem mov.", f"{mediana_dias:.0f}")


# ==========================================================
# GRÁFICOS
# ==========================================================
def render_charts(df: pd.DataFrame):
    col1, col2 = st.columns(2)

    # Top bases
    with col1:
        if COL_BASE_ENT in df.columns:
            g = df.groupby(COL_BASE_ENT)[COL_QTD].sum() if COL_QTD in df.columns else df.groupby(COL_BASE_ENT).size()
            g = g.sort_values(ascending=False).head(10).reset_index()
            g.columns = [COL_BASE_ENT, "Volume"]

            fig = px.bar(g, x=COL_BASE_ENT, y="Volume", title="Top 10 bases de entrega (sem movimentação)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Coluna de Base de entrega não encontrada.")

    # Top unidades
    with col2:
        if COL_UNIDADE in df.columns:
            g = df.groupby(COL_UNIDADE)[COL_QTD].sum() if COL_QTD in df.columns else df.groupby(COL_UNIDADE).size()
            g = g.sort_values(ascending=False).head(10).reset_index()
            g.columns = [COL_UNIDADE, "Volume"]

            fig = px.bar(g, x=COL_UNIDADE, y="Volume", title="Top 10 unidades responsáveis")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Coluna de Unidade responsável não encontrada.")

    st.divider()

    c3, c4 = st.columns([1.1, 1.4])

    # Distribuição de dias
    with c3:
        if COL_DIAS_SEM_MOV in df.columns:
            tmp = df[[COL_DIAS_SEM_MOV]].dropna()
            if not tmp.empty:
                fig = px.histogram(tmp, x=COL_DIAS_SEM_MOV, nbins=30, title="Distribuição de dias sem movimentação")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Sem valores válidos para dias sem movimentação.")
        else:
            st.info("Coluna de dias sem movimentação não disponível.")

    # Tendência por data da última operação
    with c4:
        if COL_HORA_ULT in df.columns:
            tmp = df[[COL_HORA_ULT, COL_QTD]].copy() if COL_QTD in df.columns else df[[COL_HORA_ULT]].copy()
            tmp["Data"] = tmp[COL_HORA_ULT].dt.date

            if COL_QTD in df.columns:
                daily = tmp.groupby("Data")[COL_QTD].sum().reset_index(name="Volume")
            else:
                daily = tmp.groupby("Data").size().reset_index(name="Volume")

            fig = px.line(daily, x="Data", y="Volume", markers=True, title="Volume por data da última operação")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Coluna de horário da última operação não encontrada.")


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
# CAMADA DE FONTE DE DADOS (PREPARAÇÃO PARA DB)
# ==========================================================
def load_data_file_mode(mode: str) -> pd.DataFrame:
    df = pd.DataFrame()

    if mode == "Pasta local (Windows)":
        if "folder_path" not in st.session_state:
            st.session_state.folder_path = ""

        col_a, col_b = st.columns([3, 1])

        with col_a:
            folder_input = st.text_input(
                "Caminho da pasta com os Excel",
                value=st.session_state.folder_path,
                placeholder=r"C:\Users\...\Sem Movimentacao"
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

    return df


# ==========================================================
# APP
# ==========================================================
st.markdown(
    """
    <div class="section-card">
        <h2>Pedidos sem Movimentação</h2>
        <p style="opacity:0.85;">
            Versão 1: leitura de arquivos Excel com padronização automática do seu modelo e cálculo de dias sem movimentação.
            Versão 2 (próximo passo): mesma tela, trocando a fonte para PostgreSQL.
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

st.sidebar.subheader("Fonte dos dados")
mode = st.sidebar.radio(
    "Como carregar agora?",
    ["Pasta local (Windows)", "Upload de arquivos"],
    index=1
)

df = load_data_file_mode(mode)

if df.empty:
    st.error("Não foi possível carregar dados válidos.")
    st.stop()

# Filtros
df_f = apply_filters(df)

# Tabs
tab1, tab2 = st.tabs(["Visão Geral", "Detalhe"])

with tab1:
    st.subheader("KPIs")
    build_kpis(df_f)

    st.divider()
    st.subheader("Análises principais")
    render_charts(df_f)

with tab2:
    st.subheader("Tabela detalhada")

    # Colunas sugeridas para exibir primeiro
    priority = [
        COL_PEDIDO, COL_QTD, COL_UNIDADE, COL_BASE_ENT,
        COL_TIPO_ULT, COL_HORA_ULT, COL_DIAS_SEM_MOV,
        COL_PROB, COL_TIPO_PROD, COL_ORIGEM, COL_UF,
        "Regional Remetente",
        "Nome Base Remetente",
        "Regional Ultima Operacao",
        "Nome Base Ultima Operacao",
        "__arquivo_origem"
    ]
    cols_available = df_f.columns.tolist()
    default_cols = [c for c in priority if c in cols_available]

    cols_sel = st.multiselect(
        "Colunas para exibição",
        options=cols_available,
        default=default_cols
    )

    st.dataframe(df_f[cols_sel] if cols_sel else df_f, use_container_width=True)

    st.divider()
    st.subheader("Download do recorte filtrado")
    st.download_button(
        label="Baixar Excel filtrado",
        data=to_excel_bytes(df_f),
        file_name="pedidos_sem_mov_filtrado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with st.expander("Notas técnicas"):
    st.write(
        "A coluna 'Dias sem movimentacao' é calculada com base em "
        "'Horario Ultima Operacao'. Se sua fonte mudar o nome do header, "
        "basta adicionar o novo nome no COL_MAP."
    )
