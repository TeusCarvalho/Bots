# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="SLA | Motorista, Cidade e Base",
    layout="wide",
)

PASTA_DADOS = Path(
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\03 - SLA - Entrega Realizada LM"
)

COL_REMESSA = "Remessa"
COL_BASE = "Base de entrega"
COL_PREVISTA = "Data prevista de entrega"
COL_ENTREGADOR = "Entregador"
COL_ENTREGA = "Horário da entrega"
COL_CIDADE = "Cidade Destino"

# ============================================================
# ESTILO
# ============================================================
st.markdown(
    """
    <style>
        .stApp {
            background-color: #f5f5f5;
        }

        .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
            max-width: 100%;
        }

        .topo {
            background: #b53636;
            color: white;
            text-align: center;
            font-size: 2rem;
            font-weight: 700;
            padding: 0.8rem;
            border-radius: 12px;
            margin-bottom: 1rem;
        }

        .card {
            background: white;
            border: 1px solid #e4e4e4;
            border-radius: 14px;
            padding: 1rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        }

        .kpi {
            background: #f8f8f8;
            border: 1px solid #ececec;
            border-radius: 12px;
            padding: 1rem;
            text-align: center;
        }

        .kpi-titulo {
            font-size: 0.95rem;
            color: #666;
            font-weight: 600;
            margin-bottom: 0.3rem;
        }

        .kpi-valor {
            font-size: 1.9rem;
            font-weight: 800;
            color: #222;
        }

        .verde {
            color: #0c8f4f;
        }

        .vermelho {
            color: #c52d2d;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# HELPERS
# ============================================================
def fmt_int(valor) -> str:
    if pd.isna(valor):
        return "-"
    return f"{int(valor):,}".replace(",", ".")


def fmt_pct(valor) -> str:
    if pd.isna(valor):
        return "-"
    return f"{float(valor):.2f}%".replace(".", ",")


def listar_arquivos(pasta: Path) -> list[Path]:
    arquivos = []
    for ext in ("*.xlsx", "*.xls", "*.csv"):
        arquivos.extend(pasta.glob(ext))
    return [a for a in arquivos if a.is_file() and not a.name.startswith("~$")]


def ler_arquivo(arquivo: Path) -> pd.DataFrame:
    if arquivo.suffix.lower() in [".xlsx", ".xls"]:
        return pd.read_excel(arquivo)

    if arquivo.suffix.lower() == ".csv":
        for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
            try:
                return pd.read_csv(
                    arquivo,
                    encoding=enc,
                    sep=None,
                    engine="python",
                    low_memory=False,
                )
            except Exception:
                continue

    raise ValueError(f"Não foi possível ler o arquivo: {arquivo.name}")


def validar_colunas(df: pd.DataFrame) -> None:
    obrigatorias = [
        COL_REMESSA,
        COL_BASE,
        COL_PREVISTA,
        COL_ENTREGADOR,
        COL_ENTREGA,
        COL_CIDADE,
    ]

    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        raise ValueError(f"Colunas ausentes: {faltando}")


@st.cache_data(show_spinner="Carregando base...")
def carregar_base() -> tuple[pd.DataFrame, list[str]]:
    arquivos = listar_arquivos(PASTA_DADOS)

    if not arquivos:
        return pd.DataFrame(), []

    frames = []

    for arquivo in arquivos:
        try:
            df = ler_arquivo(arquivo)
            if df.empty:
                continue

            validar_colunas(df)
            df["__arquivo__"] = arquivo.name
            frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(), []

    bruto = pd.concat(frames, ignore_index=True, sort=False).copy()

    df = pd.DataFrame()
    df["remessa"] = bruto[COL_REMESSA].astype(str).str.strip()
    df["base"] = bruto[COL_BASE].astype(str).str.strip()
    df["entregador"] = bruto[COL_ENTREGADOR].astype(str).str.strip()
    df["cidade"] = bruto[COL_CIDADE].astype(str).str.strip()
    df["arquivo"] = bruto["__arquivo__"].astype(str)

    df["data_prevista"] = pd.to_datetime(
        bruto[COL_PREVISTA],
        errors="coerce",
        dayfirst=True,
    ).dt.date

    df["data_entrega"] = pd.to_datetime(
        bruto[COL_ENTREGA],
        errors="coerce",
        dayfirst=True,
    ).dt.date

    # Higienização
    df["remessa"] = df["remessa"].replace({"": pd.NA, "nan": pd.NA})
    df["base"] = df["base"].replace({"": "SEM BASE", "nan": "SEM BASE"})
    df["entregador"] = df["entregador"].replace({"": "SEM MOTORISTA", "nan": "SEM MOTORISTA"})
    df["cidade"] = df["cidade"].replace({"": "SEM CIDADE", "nan": "SEM CIDADE"})

    # Remove linhas sem remessa
    df = df.dropna(subset=["remessa"]).copy()

    # Se existir remessa repetida, mantém a linha mais recente
    df["ordem_dt"] = pd.to_datetime(df["data_entrega"], errors="coerce")
    df = (
        df.sort_values(["remessa", "ordem_dt"], ascending=[True, True])
          .drop_duplicates(subset=["remessa"], keep="last")
          .drop(columns=["ordem_dt"])
          .reset_index(drop=True)
    )

    # Indicadores
    df["entregue"] = df["data_entrega"].notna().astype(int)

    df["no_prazo"] = (
        df["data_entrega"].notna()
        & df["data_prevista"].notna()
        & (df["data_entrega"] <= df["data_prevista"])
    ).astype(int)

    df["ano_previsto"] = pd.to_datetime(df["data_prevista"], errors="coerce").dt.year
    df["mes_previsto"] = pd.to_datetime(df["data_prevista"], errors="coerce").dt.month

    return df, [a.name for a in arquivos]


def resumo_geral(df: pd.DataFrame) -> dict:
    total = len(df)
    entregues = int(df["entregue"].sum())
    no_prazo = int(df["no_prazo"].sum())
    atraso = entregues - no_prazo
    pendentes = total - entregues
    sla = (no_prazo / total * 100) if total > 0 else 0

    return {
        "total": total,
        "entregues": entregues,
        "no_prazo": no_prazo,
        "atraso": atraso,
        "pendentes": pendentes,
        "sla": sla,
    }


def resumo_por(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[coluna, "Total", "Entregues", "No Prazo", "Atraso", "Pendentes", "SLA"])

    agrupado = (
        df.groupby(coluna, dropna=False)
        .agg(
            Total=("remessa", "count"),
            Entregues=("entregue", "sum"),
            No_Prazo=("no_prazo", "sum"),
        )
        .reset_index()
    )

    agrupado["Atraso"] = agrupado["Entregues"] - agrupado["No_Prazo"]
    agrupado["Pendentes"] = agrupado["Total"] - agrupado["Entregues"]
    agrupado["SLA"] = (agrupado["No_Prazo"] / agrupado["Total"] * 100).round(2)

    agrupado = agrupado.rename(columns={"No_Prazo": "No Prazo"})
    agrupado = agrupado.sort_values(["SLA", "Total"], ascending=[False, False]).reset_index(drop=True)

    return agrupado
def formatar_tabela(df: pd.DataFrame, coluna_nome: str) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["Total"] = out["Total"].apply(fmt_int)
    out["Entregues"] = out["Entregues"].apply(fmt_int)
    out["No Prazo"] = out["No Prazo"].apply(fmt_int)
    out["Atraso"] = out["Atraso"].apply(fmt_int)
    out["Pendentes"] = out["Pendentes"].apply(fmt_int)
    out["SLA"] = out["SLA"].apply(fmt_pct)
    out = out.rename(columns={coluna_nome: "Grupo"})
    return out


# ============================================================
# APP
# ============================================================
st.markdown('<div class="topo">SLA por Motorista, Cidade e Base</div>', unsafe_allow_html=True)

if not PASTA_DADOS.exists():
    st.error(f"Pasta não encontrada: {PASTA_DADOS}")
    st.stop()

df, arquivos = carregar_base()

if df.empty:
    st.error("Nenhum arquivo válido foi carregado.")
    st.stop()

st.caption(f"Pasta fixa: {PASTA_DADOS}")
st.caption(f"Arquivos carregados: {len(arquivos)}")

# ------------------------------------------------------------
# FILTROS
# ------------------------------------------------------------
anos = sorted([int(x) for x in df["ano_previsto"].dropna().unique().tolist()], reverse=True)
meses = sorted([int(x) for x in df["mes_previsto"].dropna().unique().tolist()])

with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        ano_sel = st.selectbox("Ano previsto", ["Todos"] + anos)

    base_filtro = df.copy()
    if ano_sel != "Todos":
        base_filtro = base_filtro[base_filtro["ano_previsto"] == int(ano_sel)]

    with c2:
        meses_validos = sorted([int(x) for x in base_filtro["mes_previsto"].dropna().unique().tolist()])
        mes_sel = st.selectbox("Mês previsto", ["Todos"] + meses_validos)

    if mes_sel != "Todos":
        base_filtro = base_filtro[base_filtro["mes_previsto"] == int(mes_sel)]

    with c3:
        bases = ["Todos"] + sorted(base_filtro["base"].dropna().astype(str).unique().tolist())
        base_sel = st.selectbox("Base", bases)

    if base_sel != "Todos":
        base_filtro = base_filtro[base_filtro["base"] == base_sel]

    with c4:
        cidades = ["Todos"] + sorted(base_filtro["cidade"].dropna().astype(str).unique().tolist())
        cidade_sel = st.selectbox("Cidade", cidades)

    if cidade_sel != "Todos":
        base_filtro = base_filtro[base_filtro["cidade"] == cidade_sel]

    st.markdown("</div>", unsafe_allow_html=True)

# ------------------------------------------------------------
# KPIs
# ------------------------------------------------------------
resumo = resumo_geral(base_filtro)

k1, k2, k3, k4, k5, k6 = st.columns(6)

with k1:
    st.markdown(
        f'<div class="kpi"><div class="kpi-titulo">Total de pedidos</div><div class="kpi-valor">{fmt_int(resumo["total"])}</div></div>',
        unsafe_allow_html=True,
    )

with k2:
    st.markdown(
        f'<div class="kpi"><div class="kpi-titulo">Entregues</div><div class="kpi-valor">{fmt_int(resumo["entregues"])}</div></div>',
        unsafe_allow_html=True,
    )

with k3:
    st.markdown(
        f'<div class="kpi"><div class="kpi-titulo">No prazo</div><div class="kpi-valor verde">{fmt_int(resumo["no_prazo"])}</div></div>',
        unsafe_allow_html=True,
    )

with k4:
    st.markdown(
        f'<div class="kpi"><div class="kpi-titulo">Atraso</div><div class="kpi-valor vermelho">{fmt_int(resumo["atraso"])}</div></div>',
        unsafe_allow_html=True,
    )

with k5:
    st.markdown(
        f'<div class="kpi"><div class="kpi-titulo">Pendentes</div><div class="kpi-valor">{fmt_int(resumo["pendentes"])}</div></div>',
        unsafe_allow_html=True,
    )

with k6:
    st.markdown(
        f'<div class="kpi"><div class="kpi-titulo">SLA</div><div class="kpi-valor">{fmt_pct(resumo["sla"])}</div></div>',
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)

# ------------------------------------------------------------
# TABELAS
# ------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["Por Motorista", "Por Cidade", "Por Base"])

with tab1:
    tabela_motorista = resumo_por(base_filtro, "entregador")
    st.dataframe(
        formatar_tabela(tabela_motorista, "entregador"),
        use_container_width=True,
        hide_index=True,
        height=500,
    )

with tab2:
    tabela_cidade = resumo_por(base_filtro, "cidade")
    st.dataframe(
        formatar_tabela(tabela_cidade, "cidade"),
        use_container_width=True,
        hide_index=True,
        height=500,
    )

with tab3:
    tabela_base = resumo_por(base_filtro, "base")
    st.dataframe(
        formatar_tabela(tabela_base, "base"),
        use_container_width=True,
        hide_index=True,
        height=500,
    )

# ------------------------------------------------------------
# DOWNLOAD
# ------------------------------------------------------------
with st.expander("Baixar resultados"):
    motoristas_export = resumo_por(base_filtro, "entregador")
    cidades_export = resumo_por(base_filtro, "cidade")
    bases_export = resumo_por(base_filtro, "base")

    buffer = pd.ExcelWriter("resultado_sla_temp.xlsx", engine="openpyxl")
    pd.DataFrame([resumo]).to_excel(buffer, sheet_name="Resumo Geral", index=False)
    motoristas_export.to_excel(buffer, sheet_name="Por Motorista", index=False)
    cidades_export.to_excel(buffer, sheet_name="Por Cidade", index=False)
    bases_export.to_excel(buffer, sheet_name="Por Base", index=False)
    buffer.close()

    with open("resultado_sla_temp.xlsx", "rb") as f:
        st.download_button(
            label="Baixar Excel",
            data=f.read(),
            file_name="resultado_sla_motorista_cidade_base.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

with st.expander("Regra usada no SLA"):
    st.write("No prazo = data da entrega menor ou igual à data prevista.")
    st.write("SLA = pedidos no prazo ÷ total de pedidos.")
    st.write("Se uma remessa repetir, o app mantém o registro mais recente.")