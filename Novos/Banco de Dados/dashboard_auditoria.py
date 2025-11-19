# -*- coding: utf-8 -*-

import streamlit as st
import json
import pandas as pd

# ============================================================
# 📌 CARREGAR ARQUIVO JSON
# ============================================================
ARQUIVO = "relatorio_auditoria.json"

with open(ARQUIVO, "r", encoding="utf-8") as f:
    dados = json.load(f)

# ============================================================
# 🎨 CONFIG STREAMLIT
# ============================================================
st.set_page_config(
    page_title="Auditoria das Tabelas - Base Qualidade GO",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Auditoria das Tabelas - Base de Dados Qualidade GO")
st.write("Visualização completa da auditoria realizada no ETL.")

# ============================================================
# 📊 INDICADORES PRINCIPAIS
# ============================================================
col1, col2, col3, col4 = st.columns(4)

col1.metric("📁 Pastas com Excel", dados["total_pastas_com_excel"])
col2.metric("📄 Arquivos Excel", dados["total_excels_encontrados"])
col3.metric("🗄️ Tabelas no Banco", dados["total_tabelas_no_banco"])
col4.metric("⚠️ Problemas Detectados", dados["problemas_detectados"])

st.markdown("---")

# ============================================================
# 📁 PASTAS SEM TABELA
# ============================================================
st.subheader("📁 Pastas que NÃO possuem tabela no banco")

pastas_sem_tabela = dados.get("pastas_sem_tabela", [])

if pastas_sem_tabela:
    df1 = pd.DataFrame({"Pasta sem tabela": pastas_sem_tabela})
    st.dataframe(df1, use_container_width=True)
else:
    st.success("✔ Todas as pastas têm tabela correspondente.")

st.markdown("---")

# ============================================================
# 🗄️ TABELAS SEM PASTA
# ============================================================
st.subheader("🗄️ Tabelas que NÃO possuem pasta correspondente")

tabelas_sem_pasta = dados.get("tabelas_sem_pasta", [])

if tabelas_sem_pasta:
    df2 = pd.DataFrame({"Tabela sem pasta": tabelas_sem_pasta})
    st.dataframe(df2, use_container_width=True)
else:
    st.success("✔ Todas as tabelas têm pasta correspondente.")

st.markdown("---")

# ============================================================
# 📄 DIFERENÇA DE COLUNAS
# ============================================================
st.subheader("📄 Diferenças de colunas entre Excel e Banco")

diff_colunas = dados.get("diferencas_colunas", {})

if diff_colunas:
    for tabela, diff in diff_colunas.items():
        st.warning(f"🔸 Diferenças encontradas na tabela **{tabela}**")
        st.json(diff)
else:
    st.success("✔ Nenhuma diferença de colunas encontrada.")

st.markdown("---")

# ============================================================
# 🧪 TABELAS VAZIAS
# ============================================================
st.subheader("🧪 Tabelas sem registros")

tabelas_sem_linhas = dados.get("tabelas_sem_linhas", [])

if tabelas_sem_linhas:
    df3 = pd.DataFrame({"Tabela vazia": tabelas_sem_linhas})
    st.dataframe(df3, use_container_width=True)
else:
    st.success("✔ Todas as tabelas possuem pelo menos 1 linha.")

st.markdown("---")

# ============================================================
# 📦 RESUMO FINAL EM JSON
# ============================================================
st.subheader("📦 Resumo Completo do JSON")
st.json(dados)
