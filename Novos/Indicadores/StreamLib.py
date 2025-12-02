import streamlit as st
import pandas as pd
import plotly.express as px
from io import StringIO

st.set_page_config(page_title="Retidos – Dashboard Executivo", layout="wide")

st.title("📦 Dashboard Executivo – Comparativo entre Semanas e Dias (v5.4)")

# ==========================================================
# 📂 UPLOAD DA PLANILHA
# ==========================================================
file = st.file_uploader("Selecione a planilha (.xlsx)", type="xlsx")

if file:
    df = pd.read_excel(file)
    df.columns = df.columns.str.strip().str.lower()  # limpeza

    st.write("📌 Colunas detectadas:", df.columns.tolist())

    # ==========================================================
    # 📌 COLUNAS OFICIAIS (CORRETAS)
    # ==========================================================
    semana_col = "semana"
    base_col = "nome da base de entrega"

    # ❗ ESTA coluna é SOMENTE divisor — NÃO entra em contagem
    col_critico = "qtd a entregar há mais de 10 dias"

    # ❗ SOMENTE estas entram no volume operacional
    colunas_soma = [
        "retidos até 5 dias",
        "retidos até 7 dias",
        "retidos até 10 dias",
        "qtd a entregar até 10 dias",
        "retidos há mais de 10 dias",
        "retidos até 15 dias",
        "超15天内滞留"
    ]

    # ==========================================================
    # 🎛️ FILTROS
    # ==========================================================
    semanas = sorted(df[semana_col].unique())
    bases = sorted(df[base_col].unique())

    col1, col2 = st.columns(2)
    semana_a = col1.selectbox("Semana A (Atual)", semanas)
    semana_b = col2.selectbox("Semana B (Comparação)", semanas)

    base_sel = st.selectbox("Filtrar por Base", ["Todas"] + list(bases))

    faixa_sel = st.selectbox(
        "Faixa de Dias (para comparar abaixo)",
        ["Todas"] + colunas_soma
    )

    # Filtrar
    df_A = df[df[semana_col] == semana_a].copy()
    df_B = df[df[semana_col] == semana_b].copy()

    if base_sel != "Todas":
        df_A = df_A[df_A[base_col] == base_sel]
        df_B = df_B[df_B[base_col] == base_sel]

    colunas_usadas = [faixa_sel] if faixa_sel != "Todas" else colunas_soma

    # ==========================================================
    # 📊 CÁLCULO PRINCIPAL — CORRIGIDO
    # ==========================================================
    soma_dias_A = df_A[colunas_soma].sum().sum()
    soma_dias_B = df_B[colunas_soma].sum().sum()

    soma_critico_A = df_A[col_critico].sum()
    soma_critico_B = df_B[col_critico].sum()

    # O cálculo do índice não é mais exibido, mas pode ser útil para o resumo
    indice_A = (soma_dias_A / soma_critico_A * 100) if soma_critico_A > 0 else 0
    indice_B = (soma_dias_B / soma_critico_B * 100) if soma_critico_B > 0 else 0
    delta_indice = indice_A - indice_B

    var_total = soma_dias_A - soma_dias_B
    var_total_perc = (var_total / soma_dias_B * 100) if soma_dias_B > 0 else 0

    delta_critico = soma_critico_A - soma_critico_B
    perc_critico = (delta_critico / soma_critico_B * 100) if soma_critico_B > 0 else 0

    # ==========================================================
    # 📊 BARRA EXECUTIVA — APENAS VOLUME + CRÍTICO (v5.4)
    # ==========================================================
    st.markdown("""
        <h2 style='margin-bottom:0px;'>📊 Barra Executiva – Semana A x Semana B</h2>
        <p style='color:#bbbbbb;margin-top:0px;font-size:14px;'>
            Foco em volume operacional e pedidos críticos.
        </p>
    """, unsafe_allow_html=True)

    # ----------------------
    # VOLUME OPERACIONAL
    # ----------------------
    st.markdown("### 🟩 Volume Operacional (Retidos Totais)")

    colD, colE, colF = st.columns(3)
    colD.metric("Retidos — Semana A", f"{soma_dias_A:,}")
    colE.metric("Retidos — Semana B", f"{soma_dias_B:,}")
    colF.metric("Δ Retidos", f"{var_total:,}", delta=f"{var_total_perc:.2f}%")

    st.markdown("<hr>", unsafe_allow_html=True)

    # ----------------------
    # CRÍTICO
    # ----------------------
    st.markdown("### 🔥 Total Crítico (>10 dias)")

    colG, colH, colI = st.columns(3)
    colG.metric("Crítico A (>10d)", f"{soma_critico_A:,}")
    colH.metric("Crítico B (>10d)", f"{soma_critico_B:,}")
    colI.metric("Δ Crítico", f"{delta_critico:,}", delta=f"{perc_critico:.2f}%")

    st.markdown("<hr style='border:2px solid #444;'>", unsafe_allow_html=True)

    # ==========================================================
    # 🏆 RANKING DAS BASES
    # ==========================================================
    st.subheader("🏆 Ranking de Bases – Maiores Retidos (Semana A)")

    if base_sel == "Todas":
        df_rank = df[df[semana_col] == semana_a].groupby(base_col)[colunas_soma].sum()
        df_rank["total_retidos"] = df_rank.sum(axis=1)
        df_rank = df_rank.sort_values("total_retidos", ascending=False).reset_index()

        top_n = st.slider("Top N bases", 3, 20, 10)

        df_top = df_rank.head(top_n)
        st.dataframe(df_top[[base_col, "total_retidos"]], use_container_width=True)

        fig_rank = px.bar(
            df_top,
            x="total_retidos",
            y=base_col,
            text="total_retidos",
            orientation="h",
            title=f"Top {top_n} Bases – Semana {semana_a}"
        )
        fig_rank.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_rank, use_container_width=True)
    else:
        st.info("Para ver o ranking, selecione 'Todas' no filtro de base.")

    # ==========================================================
    # 📘 COMPARATIVO POR FAIXA
    # ==========================================================
    st.subheader("📘 Comparativo por Faixa (A x B)")

    for col in colunas_usadas:
        A = df_A[col].sum()
        B = df_B[col].sum()
        var = A - B
        perc = (var / B * 100) if B > 0 else 0

        col1, col2, col3 = st.columns([1.4, 1, 1])
        col1.metric(col.title(), f"{A:,}")
        col2.metric("Δ Quantidade", f"{'🔺' if var > 0 else '🟢'} {var:,}")
        col3.metric("% Diferença", f"{perc:.2f}%" if B > 0 else "—")

        st.markdown("<hr style='margin:2px 0;'>", unsafe_allow_html=True)

    # ==========================================================
    # 📈 EVOLUÇÃO SEMANAL
    # ==========================================================
    st.header("📈 Evolução Semanal")

    df_plot = df.copy()
    if base_sel != "Todas":
        df_plot = df_plot[df_plot[base_col] == base_sel]

    df_plot = df_plot.groupby(semana_col)[colunas_soma].sum().reset_index()

    df_melt = df_plot.melt(
        id_vars=[semana_col],
        var_name="Faixa de Dias",
        value_name="Quantidade"
    )

    fig = px.line(
        df_melt,
        x=semana_col,
        y="Quantidade",
        color="Faixa de Dias",
        markers=True,
        title=f"📈 Evolução — Base: {base_sel}"
    )
    st.plotly_chart(fig, use_container_width=True)

    # ==========================================================
    # 📝 RESUMO EXECUTIVO
    # ==========================================================
    st.subheader("📝 Resumo Executivo")

    resumo = StringIO()
    resumo.write("Resumo Executivo – Comparativo de Semanas\n")
    resumo.write("----------------------------------------\n\n")

    resumo.write(f"Base: {base_sel}\n")
    resumo.write(f"Semana A: {semana_a}\n")
    resumo.write(f"Semana B: {semana_b}\n\n")

    resumo.write(f"Retidos A: {soma_dias_A:,}\n")
    resumo.write(f"Retidos B: {soma_dias_B:,}\n")
    resumo.write(f"Δ Retidos: {var_total:,} ({var_total_perc:.2f}%)\n\n")

    resumo.write(f"Crítico A: {soma_critico_A:,}\n")
    resumo.write(f"Crítico B: {soma_critico_B:,}\n")
    resumo.write(f"Δ Crítico: {delta_critico:,} ({perc_critico:.2f}%)\n\n")

    # Mantendo o índice no resumo, mesmo que não na barra
    resumo.write(f"Índice A: {indice_A:.2f}%\n")
    resumo.write(f"Índice B: {indice_B:.2f}%\n")
    resumo.write(f"Δ Índice: {delta_indice:.2f} pp\n")

    st.download_button(
        label="⬇ Baixar Resumo",
        data=resumo.getvalue(),
        file_name="resumo_executivo.txt",
        mime="text/plain"
    )

    # ==========================================================
    # 📋 TABELA RAW
    # ==========================================================
    st.subheader("📋 Dados da Semana A (Filtrados)")
    st.dataframe(df_A, use_container_width=True)


else:
    st.info("Faça upload da planilha para começar.")