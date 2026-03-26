# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from typing import Optional

import pandas as pd
import streamlit as st
import plotly.express as px


# =========================================================
# CONFIGURAÇÃO DA PÁGINA
# =========================================================
st.set_page_config(
    page_title="Falta de Bipagem",
    page_icon="📦",
    layout="wide",
)

st.title("📦 Dashboard - Falta de Bipagem")
st.caption(
    "Acompanhe as bases com maior volume de pedidos não bipados no recebimento "
    "e na saída para entrega."
)


# =========================================================
# COLUNAS
# =========================================================
COL_DATA = "Data considerada"
COL_BASE = "Nome da base"
COL_REC = "Qtd pedidos não bipados no recebimento"
COL_SAI = "Qtd de pedidos não bipados na saída para entrega"
COL_TOTAL_BIPAR = "Qtd pedidos a bipar"

COLUNAS_OBRIGATORIAS = [COL_DATA, COL_BASE, COL_REC, COL_SAI]


# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================
def normalizar_numero(valor) -> float:
    """Converte valores numéricos/texto para float de forma tolerante."""
    if pd.isna(valor):
        return 0.0

    if isinstance(valor, (int, float)):
        return float(valor)

    texto = str(valor).strip()

    if texto == "":
        return 0.0

    # Ajuste básico para números com separadores
    texto = texto.replace(".", "").replace(",", ".")

    try:
        return float(texto)
    except ValueError:
        return 0.0


def formatar_inteiro(valor: int | float) -> str:
    try:
        return f"{int(valor):,}".replace(",", ".")
    except Exception:
        return "0"


def formatar_pct(valor: float | int | None) -> str:
    if valor is None or pd.isna(valor):
        return "-"
    return f"{valor:.2%}".replace(".", ",")


@st.cache_data(show_spinner=False)
def carregar_arquivo(uploaded_file, sheet_name: Optional[str] = None) -> pd.DataFrame:
    nome = uploaded_file.name.lower()

    if nome.endswith(".csv"):
        try:
            df = pd.read_csv(uploaded_file, sep=None, engine="python", encoding="utf-8")
        except Exception:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, sep=None, engine="python", encoding="latin1")
        return df

    if nome.endswith(".xlsx") or nome.endswith(".xls"):
        return pd.read_excel(uploaded_file, sheet_name=sheet_name)

    raise ValueError("Formato de arquivo não suportado. Use Excel ou CSV.")


def preparar_dados(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """
    Prepara os dados e retorna:
    - df tratado
    - flag indicando se a coluna 'Qtd pedidos a bipar' existe
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    faltando = [c for c in COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltando:
        raise ValueError(
            "As seguintes colunas obrigatórias não foram encontradas: "
            + ", ".join(faltando)
        )

    usa_total_bipar = COL_TOTAL_BIPAR in df.columns

    colunas_usadas = COLUNAS_OBRIGATORIAS.copy()
    if usa_total_bipar:
        colunas_usadas.append(COL_TOTAL_BIPAR)

    df = df[colunas_usadas].copy()

    df[COL_DATA] = pd.to_datetime(df[COL_DATA], errors="coerce", dayfirst=True)
    df[COL_BASE] = df[COL_BASE].astype(str).str.strip()

    df[COL_REC] = df[COL_REC].apply(normalizar_numero)
    df[COL_SAI] = df[COL_SAI].apply(normalizar_numero)

    if usa_total_bipar:
        df[COL_TOTAL_BIPAR] = df[COL_TOTAL_BIPAR].apply(normalizar_numero)

    df = df.dropna(subset=[COL_DATA])
    df = df[df[COL_BASE] != ""].copy()

    df[COL_DATA] = df[COL_DATA].dt.normalize()

    df[COL_REC] = df[COL_REC].round(0).astype(int)
    df[COL_SAI] = df[COL_SAI].round(0).astype(int)

    if usa_total_bipar:
        df[COL_TOTAL_BIPAR] = df[COL_TOTAL_BIPAR].round(0).astype(int)

    return df, usa_total_bipar


def consolidar_periodo(df: pd.DataFrame, usa_total_bipar: bool) -> pd.DataFrame:
    """Agrupa por data e base."""
    colunas_soma = [COL_REC, COL_SAI]
    if usa_total_bipar:
        colunas_soma.append(COL_TOTAL_BIPAR)

    base = (
        df.groupby([COL_DATA, COL_BASE], as_index=False)[colunas_soma]
        .sum()
        .sort_values([COL_DATA, COL_BASE])
    )

    base["Total faltas"] = base[COL_REC] + base[COL_SAI]

    if usa_total_bipar:
        base["Pct recebimento"] = base.apply(
            lambda x: x[COL_REC] / x[COL_TOTAL_BIPAR] if x[COL_TOTAL_BIPAR] > 0 else 0,
            axis=1,
        )
        base["Pct saída"] = base.apply(
            lambda x: x[COL_SAI] / x[COL_TOTAL_BIPAR] if x[COL_TOTAL_BIPAR] > 0 else 0,
            axis=1,
        )

    return base


def resumo_por_base(df_consolidado: pd.DataFrame, usa_total_bipar: bool) -> pd.DataFrame:
    colunas = [COL_REC, COL_SAI, "Total faltas"]
    if usa_total_bipar:
        colunas.append(COL_TOTAL_BIPAR)

    base = (
        df_consolidado.groupby(COL_BASE, as_index=False)[colunas]
        .sum()
        .sort_values("Total faltas", ascending=False)
    )

    if usa_total_bipar:
        base["Pct recebimento"] = base.apply(
            lambda x: x[COL_REC] / x[COL_TOTAL_BIPAR] if x[COL_TOTAL_BIPAR] > 0 else 0,
            axis=1,
        )
        base["Pct saída"] = base.apply(
            lambda x: x[COL_SAI] / x[COL_TOTAL_BIPAR] if x[COL_TOTAL_BIPAR] > 0 else 0,
            axis=1,
        )

    return base


def resumo_diario(df_consolidado: pd.DataFrame, usa_total_bipar: bool) -> pd.DataFrame:
    colunas = [COL_REC, COL_SAI, "Total faltas"]
    if usa_total_bipar:
        colunas.append(COL_TOTAL_BIPAR)

    diario = (
        df_consolidado.groupby(COL_DATA, as_index=False)[colunas]
        .sum()
        .sort_values(COL_DATA)
    )

    if usa_total_bipar:
        diario["Pct recebimento"] = diario.apply(
            lambda x: x[COL_REC] / x[COL_TOTAL_BIPAR] if x[COL_TOTAL_BIPAR] > 0 else 0,
            axis=1,
        )
        diario["Pct saída"] = diario.apply(
            lambda x: x[COL_SAI] / x[COL_TOTAL_BIPAR] if x[COL_TOTAL_BIPAR] > 0 else 0,
            axis=1,
        )

    return diario


def calcular_delta_periodo(df_consolidado: pd.DataFrame, data_ini, data_fim, usa_total_bipar: bool) -> dict:
    dias_periodo = max((data_fim - data_ini).days + 1, 1)

    atual = df_consolidado[
        (df_consolidado[COL_DATA] >= pd.Timestamp(data_ini))
        & (df_consolidado[COL_DATA] <= pd.Timestamp(data_fim))
    ]

    ini_ant = pd.Timestamp(data_ini) - pd.Timedelta(days=dias_periodo)
    fim_ant = pd.Timestamp(data_ini) - pd.Timedelta(days=1)

    anterior = df_consolidado[
        (df_consolidado[COL_DATA] >= ini_ant)
        & (df_consolidado[COL_DATA] <= fim_ant)
    ]

    rec_atual = int(atual[COL_REC].sum())
    sai_atual = int(atual[COL_SAI].sum())
    total_atual = int(atual["Total faltas"].sum())

    rec_ant = int(anterior[COL_REC].sum())
    sai_ant = int(anterior[COL_SAI].sum())
    total_ant = int(anterior["Total faltas"].sum())

    retorno = {
        "rec_atual": rec_atual,
        "sai_atual": sai_atual,
        "total_atual": total_atual,
        "delta_rec": rec_atual - rec_ant,
        "delta_sai": sai_atual - sai_ant,
        "delta_total": total_atual - total_ant,
        "periodo_anterior_inicio": ini_ant.date(),
        "periodo_anterior_fim": fim_ant.date(),
    }

    if usa_total_bipar:
        total_bipar_atual = int(atual[COL_TOTAL_BIPAR].sum())
        total_bipar_ant = int(anterior[COL_TOTAL_BIPAR].sum())

        pct_rec_atual = (rec_atual / total_bipar_atual) if total_bipar_atual > 0 else 0
        pct_sai_atual = (sai_atual / total_bipar_atual) if total_bipar_atual > 0 else 0

        pct_rec_ant = (rec_ant / total_bipar_ant) if total_bipar_ant > 0 else 0
        pct_sai_ant = (sai_ant / total_bipar_ant) if total_bipar_ant > 0 else 0

        retorno.update(
            {
                "total_bipar_atual": total_bipar_atual,
                "delta_total_bipar": total_bipar_atual - total_bipar_ant,
                "pct_rec_atual": pct_rec_atual,
                "pct_sai_atual": pct_sai_atual,
                "delta_pct_rec": pct_rec_atual - pct_rec_ant,
                "delta_pct_sai": pct_sai_atual - pct_sai_ant,
            }
        )

    return retorno


def encontrar_base_lider_e_dias(df_consolidado: pd.DataFrame, coluna_valor: str) -> dict:
    por_base = (
        df_consolidado.groupby(COL_BASE, as_index=False)[coluna_valor]
        .sum()
        .sort_values(coluna_valor, ascending=False)
    )

    if por_base.empty:
        return {
            "base": "-",
            "total": 0,
            "dias": [],
            "valor_dia_pico": 0,
        }

    base_lider = por_base.iloc[0][COL_BASE]
    total_base = por_base.iloc[0][coluna_valor]

    dias_base = (
        df_consolidado[df_consolidado[COL_BASE] == base_lider][[COL_DATA, coluna_valor]]
        .groupby(COL_DATA, as_index=False)[coluna_valor]
        .sum()
        .sort_values(coluna_valor, ascending=False)
    )

    if dias_base.empty:
        return {
            "base": base_lider,
            "total": total_base,
            "dias": [],
            "valor_dia_pico": 0,
        }

    pico = dias_base.iloc[0][coluna_valor]
    dias_pico = dias_base[dias_base[coluna_valor] == pico][COL_DATA].dt.strftime("%d/%m/%Y").tolist()

    return {
        "base": base_lider,
        "total": total_base,
        "dias": dias_pico,
        "valor_dia_pico": pico,
    }


def formatar_lista_dias(dias: list[str]) -> str:
    if not dias:
        return "-"
    if len(dias) <= 5:
        return ", ".join(dias)
    return ", ".join(dias[:5]) + " ..."


def gerar_excel_download(
    df_filtrado: pd.DataFrame,
    df_resumo_base: pd.DataFrame,
    df_diario: pd.DataFrame,
) -> bytes:
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_filtrado.to_excel(writer, sheet_name="Detalhe Base Dia", index=False)
        df_resumo_base.to_excel(writer, sheet_name="Resumo por Base", index=False)
        df_diario.to_excel(writer, sheet_name="Resumo Diario", index=False)

    output.seek(0)
    return output.getvalue()


# =========================================================
# UPLOAD
# =========================================================
uploaded_file = st.file_uploader(
    "Envie sua planilha Excel ou CSV",
    type=["xlsx", "xls", "csv"],
)

if not uploaded_file:
    st.info("Envie a planilha para começar a análise.")
    st.stop()


# =========================================================
# LEITURA
# =========================================================
try:
    if uploaded_file.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(uploaded_file)
        abas = xls.sheet_names

        aba_escolhida = st.selectbox(
            "Selecione a aba da planilha",
            options=abas,
            index=0,
        )

        uploaded_file.seek(0)
        df_raw = carregar_arquivo(uploaded_file, sheet_name=aba_escolhida)
    else:
        df_raw = carregar_arquivo(uploaded_file)

    df, usa_total_bipar = preparar_dados(df_raw)
    df_consolidado = consolidar_periodo(df, usa_total_bipar)

except Exception as e:
    st.error(f"Erro ao ler/preparar a planilha: {e}")
    st.stop()


# =========================================================
# FILTROS
# =========================================================
st.subheader("Filtros")

col_f1, col_f2, col_f3 = st.columns([1.2, 2, 1.2])

data_min = df_consolidado[COL_DATA].min().date()
data_max = df_consolidado[COL_DATA].max().date()

with col_f1:
    periodo = st.date_input(
        "Período",
        value=(data_min, data_max),
        min_value=data_min,
        max_value=data_max,
    )

    if isinstance(periodo, tuple) and len(periodo) == 2:
        data_ini, data_fim = periodo
    else:
        data_ini = data_fim = periodo

with col_f2:
    bases_disponiveis = sorted(df_consolidado[COL_BASE].dropna().unique().tolist())
    bases_selecionadas = st.multiselect(
        "Bases",
        options=bases_disponiveis,
        default=[],
        placeholder="Selecione uma ou mais bases. Vazio = todas",
    )

with col_f3:
    modo_ranking = st.radio(
        "Ranking por",
        options=["Quantidade", "Porcentagem"],
        horizontal=False,
        disabled=not usa_total_bipar,
    )

    if not usa_total_bipar:
        st.caption("A coluna 'Qtd pedidos a bipar' não foi encontrada. Ranking por porcentagem indisponível.")

df_filtrado = df_consolidado[
    (df_consolidado[COL_DATA] >= pd.Timestamp(data_ini))
    & (df_consolidado[COL_DATA] <= pd.Timestamp(data_fim))
].copy()

if bases_selecionadas:
    df_filtrado = df_filtrado[df_filtrado[COL_BASE].isin(bases_selecionadas)].copy()

if df_filtrado.empty:
    st.warning("Não há dados para os filtros selecionados.")
    st.stop()


# =========================================================
# RESUMOS
# =========================================================
df_resumo_base = resumo_por_base(df_filtrado, usa_total_bipar)
df_diario = resumo_diario(df_filtrado, usa_total_bipar)
comparativo = calcular_delta_periodo(df_consolidado, data_ini, data_fim, usa_total_bipar)

lider_rec = encontrar_base_lider_e_dias(df_filtrado, COL_REC)
lider_sai = encontrar_base_lider_e_dias(df_filtrado, COL_SAI)
lider_total = encontrar_base_lider_e_dias(df_filtrado, "Total faltas")

total_rec = int(df_filtrado[COL_REC].sum())
total_sai = int(df_filtrado[COL_SAI].sum())
total_geral = int(df_filtrado["Total faltas"].sum())

if usa_total_bipar:
    total_bipar = int(df_filtrado[COL_TOTAL_BIPAR].sum())
    pct_rec_total = (total_rec / total_bipar) if total_bipar > 0 else 0
    pct_sai_total = (total_sai / total_bipar) if total_bipar > 0 else 0


# =========================================================
# KPIs
# =========================================================
st.subheader("Resumo Geral")

if usa_total_bipar:
    k1, k2, k3, k4, k5 = st.columns(5)

    with k1:
        st.metric(
            "Pedidos a bipar",
            formatar_inteiro(comparativo["total_bipar_atual"]),
            delta=formatar_inteiro(comparativo["delta_total_bipar"]).replace(".", "."),
        )

    with k2:
        st.metric(
            "Não bipados no recebimento",
            formatar_inteiro(total_rec),
            delta=f"{comparativo['delta_rec']:+,}".replace(",", "."),
        )

    with k3:
        st.metric(
            "% não bipados no recebimento",
            formatar_pct(pct_rec_total),
            delta=formatar_pct(comparativo["delta_pct_rec"]),
        )

    with k4:
        st.metric(
            "Não bipados na saída",
            formatar_inteiro(total_sai),
            delta=f"{comparativo['delta_sai']:+,}".replace(",", "."),
        )

    with k5:
        st.metric(
            "% não bipados na saída",
            formatar_pct(pct_sai_total),
            delta=formatar_pct(comparativo["delta_pct_sai"]),
        )
else:
    k1, k2, k3 = st.columns(3)

    with k1:
        st.metric(
            "Não bipados no recebimento",
            formatar_inteiro(total_rec),
            delta=f"{comparativo['delta_rec']:+,}".replace(",", "."),
        )

    with k2:
        st.metric(
            "Não bipados na saída",
            formatar_inteiro(total_sai),
            delta=f"{comparativo['delta_sai']:+,}".replace(",", "."),
        )

    with k3:
        st.metric(
            "Total geral",
            formatar_inteiro(total_geral),
            delta=f"{comparativo['delta_total']:+,}".replace(",", "."),
        )

st.caption(
    f"Os deltas comparam o período selecionado com o período anterior equivalente "
    f"({comparativo['periodo_anterior_inicio'].strftime('%d/%m/%Y')} a "
    f"{comparativo['periodo_anterior_fim'].strftime('%d/%m/%Y')})."
)

k6, k7, k8 = st.columns(3)
with k6:
    st.metric("Base líder - Recebimento", lider_rec["base"])
with k7:
    st.metric("Base líder - Saída", lider_sai["base"])
with k8:
    st.metric("Base líder - Total geral", lider_total["base"])


# =========================================================
# DESTAQUES
# =========================================================
st.subheader("Bases com maior volume e dias de pico")

d1, d2, d3 = st.columns(3)

with d1:
    st.markdown("**Recebimento**")
    st.write(f"**Base:** {lider_rec['base']}")
    st.write(f"**Total no período:** {formatar_inteiro(lider_rec['total'])}")
    st.write(f"**Maior volume em um dia:** {formatar_inteiro(lider_rec['valor_dia_pico'])}")
    st.write(f"**Dia(s) de pico:** {formatar_lista_dias(lider_rec['dias'])}")

with d2:
    st.markdown("**Saída para entrega**")
    st.write(f"**Base:** {lider_sai['base']}")
    st.write(f"**Total no período:** {formatar_inteiro(lider_sai['total'])}")
    st.write(f"**Maior volume em um dia:** {formatar_inteiro(lider_sai['valor_dia_pico'])}")
    st.write(f"**Dia(s) de pico:** {formatar_lista_dias(lider_sai['dias'])}")

with d3:
    st.markdown("**Total geral**")
    st.write(f"**Base:** {lider_total['base']}")
    st.write(f"**Total no período:** {formatar_inteiro(lider_total['total'])}")
    st.write(f"**Maior volume em um dia:** {formatar_inteiro(lider_total['valor_dia_pico'])}")
    st.write(f"**Dia(s) de pico:** {formatar_lista_dias(lider_total['dias'])}")


# =========================================================
# TOP 10 BASES
# =========================================================
st.subheader("Top 10 bases")

c1, c2 = st.columns(2)

if usa_total_bipar and modo_ranking == "Porcentagem":
    top10_rec = (
        df_resumo_base[[COL_BASE, "Pct recebimento"]]
        .sort_values("Pct recebimento", ascending=False)
        .head(10)
        .sort_values("Pct recebimento", ascending=True)
    )

    top10_sai = (
        df_resumo_base[[COL_BASE, "Pct saída"]]
        .sort_values("Pct saída", ascending=False)
        .head(10)
        .sort_values("Pct saída", ascending=True)
    )

    with c1:
        fig_rec = px.bar(
            top10_rec,
            x="Pct recebimento",
            y=COL_BASE,
            orientation="h",
            title="Top 10 - % não bipados no recebimento",
            text="Pct recebimento",
        )
        fig_rec.update_traces(texttemplate="%{text:.2%}", textposition="outside")
        fig_rec.update_layout(height=500, yaxis_title="", xaxis_title="Percentual")
        st.plotly_chart(fig_rec, use_container_width=True)

    with c2:
        fig_sai = px.bar(
            top10_sai,
            x="Pct saída",
            y=COL_BASE,
            orientation="h",
            title="Top 10 - % não bipados na saída para entrega",
            text="Pct saída",
        )
        fig_sai.update_traces(texttemplate="%{text:.2%}", textposition="outside")
        fig_sai.update_layout(height=500, yaxis_title="", xaxis_title="Percentual")
        st.plotly_chart(fig_sai, use_container_width=True)

else:
    top10_rec = (
        df_resumo_base[[COL_BASE, COL_REC]]
        .sort_values(COL_REC, ascending=False)
        .head(10)
        .sort_values(COL_REC, ascending=True)
    )

    top10_sai = (
        df_resumo_base[[COL_BASE, COL_SAI]]
        .sort_values(COL_SAI, ascending=False)
        .head(10)
        .sort_values(COL_SAI, ascending=True)
    )

    with c1:
        fig_rec = px.bar(
            top10_rec,
            x=COL_REC,
            y=COL_BASE,
            orientation="h",
            title="Top 10 - Não bipados no recebimento",
            text=COL_REC,
        )
        fig_rec.update_traces(textposition="outside")
        fig_rec.update_layout(height=500, yaxis_title="", xaxis_title="Quantidade")
        st.plotly_chart(fig_rec, use_container_width=True)

    with c2:
        fig_sai = px.bar(
            top10_sai,
            x=COL_SAI,
            y=COL_BASE,
            orientation="h",
            title="Top 10 - Não bipados na saída para entrega",
            text=COL_SAI,
        )
        fig_sai.update_traces(textposition="outside")
        fig_sai.update_layout(height=500, yaxis_title="", xaxis_title="Quantidade")
        st.plotly_chart(fig_sai, use_container_width=True)


# =========================================================
# EVOLUÇÃO DIÁRIA
# =========================================================
st.subheader("Evolução diária")

g1, g2 = st.columns(2)

with g1:
    fig_evol = px.line(
        df_diario,
        x=COL_DATA,
        y=[COL_REC, COL_SAI],
        markers=True,
        title="Recebimento x Saída por dia",
    )
    fig_evol.update_layout(
        height=450,
        xaxis_title="Data",
        yaxis_title="Quantidade",
        legend_title="Indicador",
    )
    st.plotly_chart(fig_evol, use_container_width=True)

with g2:
    if usa_total_bipar:
        metrica_evol = st.radio(
            "Visualizar evolução percentual",
            options=["Quantidade", "Porcentagem"],
            horizontal=True,
            key="radio_evol_pct",
        )
    else:
        metrica_evol = "Quantidade"

    if usa_total_bipar and metrica_evol == "Porcentagem":
        fig_pct = px.line(
            df_diario,
            x=COL_DATA,
            y=["Pct recebimento", "Pct saída"],
            markers=True,
            title="% não bipados por dia",
        )
        fig_pct.update_layout(
            height=450,
            xaxis_title="Data",
            yaxis_title="Percentual",
            legend_title="Indicador",
        )
        fig_pct.update_yaxes(tickformat=".2%")
        st.plotly_chart(fig_pct, use_container_width=True)
    else:
        fig_total = px.line(
            df_diario,
            x=COL_DATA,
            y="Total faltas",
            markers=True,
            title="Total geral por dia",
        )
        fig_total.update_layout(
            height=450,
            xaxis_title="Data",
            yaxis_title="Quantidade",
        )
        st.plotly_chart(fig_total, use_container_width=True)


# =========================================================
# ACOMPANHAMENTO POR BASE
# =========================================================
st.subheader("Acompanhamento por base")

bases_top_geral = df_resumo_base[COL_BASE].head(10).tolist()
bases_grafico = st.multiselect(
    "Selecione até 5 bases para acompanhar no gráfico",
    options=df_resumo_base[COL_BASE].tolist(),
    default=bases_top_geral[:3],
    max_selections=5,
)

opcoes_modo = ["Total geral", "Recebimento", "Saída para entrega"]
if usa_total_bipar:
    opcoes_modo.extend(["% Recebimento", "% Saída"])

modo_grafico = st.radio(
    "Indicador do gráfico por base",
    options=opcoes_modo,
    horizontal=True,
)

if bases_grafico:
    df_plot_base = df_filtrado[df_filtrado[COL_BASE].isin(bases_grafico)].copy()

    mapa_coluna = {
        "Total geral": "Total faltas",
        "Recebimento": COL_REC,
        "Saída para entrega": COL_SAI,
        "% Recebimento": "Pct recebimento",
        "% Saída": "Pct saída",
    }
    coluna_plot = mapa_coluna[modo_grafico]

    fig_bases = px.line(
        df_plot_base,
        x=COL_DATA,
        y=coluna_plot,
        color=COL_BASE,
        markers=True,
        title=f"Acompanhamento diário por base - {modo_grafico}",
    )
    fig_bases.update_layout(
        height=500,
        xaxis_title="Data",
        yaxis_title="Valor",
        legend_title="Base",
    )

    if coluna_plot in ["Pct recebimento", "Pct saída"]:
        fig_bases.update_yaxes(tickformat=".2%")

    st.plotly_chart(fig_bases, use_container_width=True)
else:
    st.info("Selecione pelo menos uma base para visualizar o acompanhamento.")


# =========================================================
# TABELAS
# =========================================================
st.subheader("Tabelas")

tab1, tab2, tab3 = st.tabs(
    ["Resumo por base", "Resumo diário", "Detalhe base x dia"]
)

with tab1:
    exibir_base = df_resumo_base.copy()

    if usa_total_bipar:
        exibir_base["Pct recebimento"] = exibir_base["Pct recebimento"].map(formatar_pct)
        exibir_base["Pct saída"] = exibir_base["Pct saída"].map(formatar_pct)

    exibir_base = exibir_base.rename(
        columns={
            COL_BASE: "Base",
            COL_REC: "Não bipados no recebimento",
            COL_SAI: "Não bipados na saída",
            COL_TOTAL_BIPAR: "Pedidos a bipar",
            "Total faltas": "Total geral",
            "Pct recebimento": "% recebimento",
            "Pct saída": "% saída",
        }
    )
    st.dataframe(exibir_base, use_container_width=True, hide_index=True)

with tab2:
    exibir_diario = df_diario.copy()
    exibir_diario[COL_DATA] = exibir_diario[COL_DATA].dt.strftime("%d/%m/%Y")

    if usa_total_bipar:
        exibir_diario["Pct recebimento"] = exibir_diario["Pct recebimento"].map(formatar_pct)
        exibir_diario["Pct saída"] = exibir_diario["Pct saída"].map(formatar_pct)

    exibir_diario = exibir_diario.rename(
        columns={
            COL_DATA: "Data",
            COL_REC: "Não bipados no recebimento",
            COL_SAI: "Não bipados na saída",
            COL_TOTAL_BIPAR: "Pedidos a bipar",
            "Total faltas": "Total geral",
            "Pct recebimento": "% recebimento",
            "Pct saída": "% saída",
        }
    )
    st.dataframe(exibir_diario, use_container_width=True, hide_index=True)

with tab3:
    exibir_detalhe = df_filtrado.copy()
    exibir_detalhe[COL_DATA] = exibir_detalhe[COL_DATA].dt.strftime("%d/%m/%Y")

    if usa_total_bipar:
        exibir_detalhe["Pct recebimento"] = exibir_detalhe["Pct recebimento"].map(formatar_pct)
        exibir_detalhe["Pct saída"] = exibir_detalhe["Pct saída"].map(formatar_pct)

    exibir_detalhe = exibir_detalhe.sort_values(
        by=["Total faltas", COL_DATA],
        ascending=[False, True],
    )

    exibir_detalhe = exibir_detalhe.rename(
        columns={
            COL_DATA: "Data",
            COL_BASE: "Base",
            COL_REC: "Não bipados no recebimento",
            COL_SAI: "Não bipados na saída",
            COL_TOTAL_BIPAR: "Pedidos a bipar",
            "Total faltas": "Total geral",
            "Pct recebimento": "% recebimento",
            "Pct saída": "% saída",
        }
    )
    st.dataframe(exibir_detalhe, use_container_width=True, hide_index=True)


# =========================================================
# DOWNLOAD
# =========================================================
st.subheader("Download")

excel_bytes = gerar_excel_download(
    df_filtrado=df_filtrado,
    df_resumo_base=df_resumo_base,
    df_diario=df_diario,
)

st.download_button(
    label="⬇️ Baixar resultado em Excel",
    data=excel_bytes,
    file_name="falta_bipagem_dashboard.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)